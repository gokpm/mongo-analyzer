package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

var outputPrefix string
var inputFile *string
var outputDir *string
var chunkSize *int
var queryProf = map[string]profMeta{}
var collectionProf = map[string]profMeta{}

var commands = [][]string{}
var commandsCounter = 0
var collscans = [][]string{}
var collscansCounter = 0

func main() {
	inputFile = flag.String("i", "", "input file path")
	outputDir = flag.String("o", "", "output folder path")
	chunkSize = flag.Int("s", 0, "chunk size")
	flag.Parse()
	if inputFile == nil || *inputFile == "" {
		flag.Usage()
		log.Fatalln("error: -i flag (input file) is required")
	}
	if outputDir == nil || *outputDir == "" {
		flag.Usage()
		log.Fatalln("error: -o flag (output folder) is required")
	}
	if chunkSize == nil {
		flag.Usage()
		log.Fatalln("error: -s flag (chunk size) is required")
	}
	if *chunkSize < 100 {
		flag.Usage()
		log.Fatalln("error: chunk size (-s) must be atleast 100")
	}
	fileName := filepath.Base(*inputFile)
	outputPrefix = strings.TrimSuffix(fileName, filepath.Ext(fileName))
	data, err := read(*inputFile)
	if err != nil {
		log.Fatalln(err)
	}
	data = bytes.TrimSpace(data)
	lines := bytes.Split(data, []byte("\n"))
	if len(lines) > 0 {
		err = os.MkdirAll(*outputDir, os.ModePerm)
		if err != nil {
			log.Fatalln(err)
		}
	} else {
		return
	}
	for skip := 0; skip < len(lines); skip += *chunkSize {
		limit := skip + *chunkSize
		if limit > len(lines) {
			limit = len(lines)
		}
		chunk := lines[skip:limit]
		err := process(skip, chunk)
		if err != nil {
			log.Fatalln(err)
		}
	}
	if len(commands) > 0 {
		fileName := *outputDir + "/" + outputPrefix + "_commands" + "_" + fmt.Sprint(commandsCounter) + ".csv"
		err := saveCommands(commands, fileName)
		if err != nil {
			log.Fatalln(err)
		}
	}
	if len(collscans) > 0 {
		fileName := *outputDir + "/" + outputPrefix + "_collscans" + "_" + fmt.Sprint(collscansCounter) + ".csv"
		err := saveCommands(collscans, fileName)
		if err != nil {
			log.Fatalln(err)
		}
	}
	err = saveQueryProf()
	if err != nil {
		log.Fatalln(err)
	}
	err = saveCollectionProf()
	if err != nil {
		log.Fatalln(err)
	}
}

func process(i int, chunk [][]byte) error {
	records, err := parse(chunk)
	if err != nil {
		return err
	}
	err = chunkConvert(i, records)
	if err != nil {
		return err
	}
	err = chunkCommands(records)
	if err != nil {
		return err
	}
	err = chunkQueryProf(records)
	if err != nil {
		return err
	}
	err = chunkCollectionProf(records)
	if err != nil {
		return err
	}
	return nil
}

func chunkConvert(i int, records []map[string]any) error {
	rows := [][]string{}
	for _, record := range records {
		row := []string{
			fmt.Sprintf("%[1]v", record["t"]),
			fmt.Sprintf("%[1]v", record["s"]),
			fmt.Sprintf("%[1]v", record["c"]),
			fmt.Sprintf("%[1]v", record["id"]),
			fmt.Sprintf("%[1]v", record["ctx"]),
			fmt.Sprintf("%[1]v", record["msg"]),
			fmt.Sprintf("%[1]v", record["attr"]),
		}
		rows = append(rows, row)
	}
	if len(rows) < 1 {
		return nil
	}
	counter := i / *chunkSize
	fileName := *outputDir + "/" + outputPrefix + "_logs" + "_" + fmt.Sprint(counter) + ".csv"
	_, err := os.Stat(fileName)
	if err != nil {
		if os.IsNotExist(err) {
			header := []string{
				"t",
				"s",
				"c",
				"id",
				"ctx",
				"msg",
				"attr",
			}
			rows = append([][]string{header}, rows...)
			err = nil
		} else {
			return err
		}
	}
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, os.ModePerm)
	if err != nil {
		return err
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	err = writer.WriteAll(rows)
	if err != nil {
		return err
	}
	writer.Flush()
	return nil
}

func chunkCommands(records []map[string]any) error {
	for _, record := range records {
		value, ok := record["msg"]
		if !ok {
			return errors.New("msg: invalid key")
		}
		msg, ok := value.(string)
		if !ok {
			return errors.New("msg: invalid string")
		}
		if msg == "Slow query" {
			value, ok := record["t"]
			if !ok {
				return errors.New("t: invalid key")
			}
			t, ok := value.(map[string]any)
			if !ok {
				return errors.New("t: invalid map")
			}
			value, ok = t["$date"]
			if !ok {
				return errors.New("$date: invalid key")
			}
			date, ok := value.(string)
			if !ok {
				return errors.New("$date: invalid string")
			}
			timestamp, err := time.Parse(time.RFC3339, date)
			if err != nil {
				return err
			}
			value, ok = record["attr"]
			if !ok {
				return errors.New("attr: invalid key")
			}
			attr, ok := value.(map[string]any)
			if !ok {
				return errors.New("attr: invalid map")
			}
			queryHash := ""
			value, ok = attr["queryHash"]
			if ok {
				queryHash, ok = value.(string)
				if !ok {
					return errors.New("queryHash: invalid string")
				}
			}
			value, ok = attr["durationMillis"]
			if !ok {
				return errors.New("durationMillis: invalid key")
			}
			durationMillis, ok := value.(float64)
			if !ok {
				return errors.New("durationMillis: invalid float64")
			}
			duration := durationMillis / (1000 * 60)
			value, ok = attr["command"]
			if !ok {
				return errors.New("command: invalid key")
			}
			command, ok := value.(map[string]any)
			if !ok {
				return errors.New("command: invalid map")
			}
			delete(command, "$clusterTime")
			delete(command, "$db")
			delete(command, "$readPreference")
			delete(command, "lsid")
			delete(command, "readConcern")
			delete(command, "writeConcern")
			delete(command, "txnNumber")
			delete(command, "flowControl")
			var commandBytes []byte
			commandBytes, err = json.Marshal(command)
			if err != nil {
				return err
			}
			value, ok = attr["type"]
			if !ok {
				return errors.New("type: invalid key")
			}
			typeStr, ok := value.(string)
			if !ok {
				return errors.New("type: invalid string")
			}
			sort := false
			value, ok = attr["hasSortStage"]
			if ok {
				sort, ok = value.(bool)
				if !ok {
					return errors.New("hasSortStage: invalid bool")
				}
			}
			appName := ""
			value, ok = attr["appName"]
			if ok {
				appName, ok = value.(string)
				if !ok {
					return errors.New("appName: invalid string")
				}
			}
			remote := ""
			value, ok = attr["remote"]
			if ok {
				remote, ok = value.(string)
				if !ok {
					return errors.New("remote: invalid string")
				}
			}
			planSummary := ""
			value, ok = attr["planSummary"]
			if ok {
				planSummary, ok = value.(string)
				if !ok {
					return errors.New("planSummary: invalid string")
				}
			}
			value, ok = attr["ns"]
			if !ok {
				return errors.New("ns: invalid key")
			}
			ns, ok := value.(string)
			if !ok {
				return errors.New("ns: invalid string")
			}
			splitStr := strings.Split(ns, ".")
			var database string
			if len(splitStr) > 1 {
				database = splitStr[0]
			}
			var collection string
			if len(splitStr) > 1 {
				collection = splitStr[1]
			}
			row := []string{
				queryHash,
				fmt.Sprintf("%.2f", duration),
				timestamp.Format(time.RFC3339),
				appName,
				remote,
				database,
				collection,
				typeStr,
				fmt.Sprint(sort),
				planSummary,
				string(commandBytes),
			}
			err = addCommand(row)
			if err != nil {
				return err
			}
			if planSummary == "COLLSCAN" {
				err = addCollscan(row)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func addCommand(row []string) error {
	commands = append(commands, row)
	if len(commands) < *chunkSize {
		return nil
	}
	fileName := *outputDir + "/" + outputPrefix + "_commands" + "_" + fmt.Sprint(commandsCounter) + ".csv"
	err := saveCommands(commands, fileName)
	if err != nil {
		return err
	}
	commands = [][]string{}
	commandsCounter++
	return nil
}

func addCollscan(row []string) error {
	collscans = append(collscans, row)
	if len(collscans) < *chunkSize {
		return nil
	}
	fileName := *outputDir + "/" + outputPrefix + "_collscans" + "_" + fmt.Sprint(collscansCounter) + ".csv"
	err := saveCommands(collscans, fileName)
	if err != nil {
		return err
	}
	collscans = [][]string{}
	collscansCounter++
	return nil
}

func saveCommands(commands [][]string, fileName string) error {
	_, err := os.Stat(fileName)
	if err != nil {
		if os.IsNotExist(err) {
			header := []string{
				"Hash",
				"Duration (Minutes)",
				"Time (IST)",
				"Application",
				"Origin",
				"Database",
				"Collection",
				"Type",
				"Sort",
				"Plan",
				"Command",
			}
			commands = append([][]string{header}, commands...)
			err = nil
		} else {
			return err
		}
	}
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, os.ModePerm)
	if err != nil {
		return err
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	err = writer.WriteAll(commands)
	if err != nil {
		return err
	}
	writer.Flush()
	return nil
}

func read(input string) ([]byte, error) {
	output, err := os.ReadFile(input)
	if err != nil {
		return nil, err
	}
	return output, nil
}

func parse(lines [][]byte) ([]map[string]any, error) {
	records := []map[string]any{}
	for i := range lines {
		record := map[string]any{}
		err := json.Unmarshal(lines[i], &record)
		if err != nil {
			err = nil
			continue
		}
		records = append(records, record)
	}
	return records, nil
}

func save(fileName string, rows [][]string) error {
	file, err := os.Create(*outputDir + "/" + fileName)
	if err != nil {
		return err
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	err = writer.WriteAll(rows)
	if err != nil {
		return err
	}
	writer.Flush()
	return nil
}

type profMeta struct {
	docsExamined float64
	nreturned    float64
	reslen       float64
	duration     float64
	count        int64
	min          float64
	max          float64
}

type profElement struct {
	key   string
	value profMeta
}

func chunkQueryProf(records []map[string]any) error {
	for _, record := range records {
		value, ok := record["msg"]
		if !ok {
			return errors.New("msg: invalid key")
		}
		msg, ok := value.(string)
		if !ok {
			return errors.New("msg: invalid string")
		}
		if msg == "Slow query" {
			value, ok := record["attr"]
			if !ok {
				return errors.New("attr: invalid key")
			}
			attr, ok := value.(map[string]any)
			if !ok {
				return errors.New("attr: invalid map")
			}
			queryHash := ""
			value, ok = attr["queryHash"]
			if ok {
				queryHash, ok = value.(string)
				if !ok {
					return errors.New("queryHash: invalid string")
				}
			}
			value, ok = attr["durationMillis"]
			if !ok {
				return errors.New("durationMillis: invalid key")
			}
			durationMillis, ok := value.(float64)
			if !ok {
				return errors.New("durationMillis: invalid float64")
			}
			duration := durationMillis / (1000 * 60)
			docsExamined := float64(0)
			value, ok = attr["docsExamined"]
			if ok {
				docsExamined, ok = value.(float64)
				if !ok {
					return errors.New("docsExamined: invalid float64")
				}
			}
			nreturned := float64(0)
			value, ok = attr["nreturned"]
			if ok {
				nreturned, ok = value.(float64)
				if !ok {
					return errors.New("nreturned: invalid float64")
				}
			}

			reslen := float64(0)
			value, ok = attr["reslen"]
			if ok {
				reslen, ok = value.(float64)
				if !ok {
					return errors.New("reslen: invalid float64")
				}
			}
			meta, ok := queryProf[queryHash]
			if !ok {
				meta.min = math.MaxFloat64
			}
			if durationMillis < meta.min {
				meta.min = durationMillis
			}
			if durationMillis > meta.max {
				meta.max = durationMillis
			}
			meta.count++
			meta.duration += duration
			meta.docsExamined += docsExamined
			meta.nreturned += nreturned
			meta.reslen += reslen / (1024 * 1024)
			queryProf[queryHash] = meta
		}
	}
	return nil
}

func saveQueryProf() error {
	if len(queryProf) < 1 {
		return nil
	}
	sorted := []profElement{}
	var total float64
	for key, value := range queryProf {
		total += value.duration
		sorted = append(sorted, profElement{key, value})
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].value.duration > sorted[j].value.duration
	})
	header := []string{
		"Hash",
		"Count",
		"Duration (Minutes)",
		"Percentage (Duration)",
		"Average Latency (ms)",
		"Minimum Latency (ms)",
		"Maximum Latency (ms)",
		"Examined",
		"Returned",
		"Ratio (Examined/Returned)",
		"Response (MB)",
		"Throughput (MB/Second)",
	}
	rows := [][]string{header}
	for _, element := range sorted {
		ratio := float64(element.value.docsExamined) / float64(element.value.nreturned)
		if element.value.nreturned == 0 {
			ratio = math.MaxFloat64
		}
		row := []string{
			element.key,
			fmt.Sprint(element.value.count),
			fmt.Sprintf("%0.2[1]f", element.value.duration),
			fmt.Sprintf("%0.2[1]f", (element.value.duration/total)*100),
			fmt.Sprintf("%0.2[1]f", (element.value.duration*60*1000)/float64(element.value.count)),
			fmt.Sprintf("%0.2[1]f", element.value.min),
			fmt.Sprintf("%0.2[1]f", element.value.max),
			fmt.Sprint(element.value.docsExamined),
			fmt.Sprint(element.value.nreturned),
			fmt.Sprintf("%0.2[1]f", ratio),
			fmt.Sprintf("%0.2[1]f", element.value.reslen),
			fmt.Sprintf("%0.2[1]f", element.value.reslen/(element.value.duration*60)),
		}
		rows = append(rows, row)
	}
	err := save(outputPrefix+"_query_prof.csv", rows)
	if err != nil {
		return err
	}
	return nil
}

func chunkCollectionProf(records []map[string]any) error {
	for _, record := range records {
		value, ok := record["msg"]
		if !ok {
			return errors.New("msg: invalid key")
		}
		msg, ok := value.(string)
		if !ok {
			return errors.New("msg: invalid string")
		}
		if msg == "Slow query" {
			value, ok := record["attr"]
			if !ok {
				return errors.New("attr: invalid key")
			}
			attr, ok := value.(map[string]any)
			if !ok {
				return errors.New("attr: invalid map")
			}
			value, ok = attr["durationMillis"]
			if !ok {
				return errors.New("durationMillis: invalid key")
			}
			durationMillis, ok := value.(float64)
			if !ok {
				return errors.New("durationMillis: invalid float64")
			}
			duration := durationMillis / (1000 * 60)
			value, ok = attr["ns"]
			if !ok {
				return errors.New("ns: invalid key")
			}
			ns, ok := value.(string)
			if !ok {
				return errors.New("ns: invalid string")
			}
			docsExamined := float64(0)
			value, ok = attr["docsExamined"]
			if ok {
				docsExamined, ok = value.(float64)
				if !ok {
					return errors.New("docsExamined: invalid float64")
				}
			}
			nreturned := float64(0)
			value, ok = attr["nreturned"]
			if ok {
				nreturned, ok = value.(float64)
				if !ok {
					return errors.New("nreturned: invalid float64")
				}
			}
			reslen := float64(0)
			value, ok = attr["reslen"]
			if ok {
				reslen, ok = value.(float64)
				if !ok {
					return errors.New("reslen: invalid float64")
				}
			}
			meta, ok := collectionProf[ns]
			if !ok {
				meta.min = math.MaxFloat64
			}
			if durationMillis < meta.min {
				meta.min = durationMillis
			}
			if durationMillis > meta.max {
				meta.max = durationMillis
			}
			meta.count++
			meta.duration += duration
			meta.docsExamined += docsExamined
			meta.nreturned += nreturned
			meta.reslen += reslen / (1024 * 1024)
			collectionProf[ns] = meta
		}
	}
	return nil
}

func saveCollectionProf() error {
	if len(collectionProf) < 1 {
		return nil
	}
	sorted := []profElement{}
	var total float64
	for key, value := range collectionProf {
		total += value.duration
		sorted = append(sorted, profElement{key, value})
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].value.duration > sorted[j].value.duration
	})
	header := []string{
		"Collection",
		"Count",
		"Duration (Minutes)",
		"Percentage (Duration)",
		"Average Latency (ms)",
		"Minimum Latency (ms)",
		"Maximum Latency (ms)",
		"Examined",
		"Returned",
		"Ratio (Examined/Returned)",
		"Response (MB)",
		"Throughput (MB/Second)",
	}
	rows := [][]string{header}
	for _, element := range sorted {
		ratio := float64(element.value.docsExamined) / float64(element.value.nreturned)
		if element.value.nreturned == 0 {
			ratio = math.MaxFloat64
		}
		row := []string{
			element.key,
			fmt.Sprint(element.value.count),
			fmt.Sprintf("%0.2[1]f", element.value.duration),
			fmt.Sprintf("%0.2[1]f", (element.value.duration/total)*100),
			fmt.Sprintf("%0.2[1]f", (element.value.duration*60*1000)/float64(element.value.count)),
			fmt.Sprintf("%0.2[1]f", element.value.min),
			fmt.Sprintf("%0.2[1]f", element.value.max),
			fmt.Sprint(element.value.docsExamined),
			fmt.Sprint(element.value.nreturned),
			fmt.Sprintf("%0.2[1]f", ratio),
			fmt.Sprintf("%0.2[1]f", element.value.reslen),
			fmt.Sprintf("%0.2[1]f", element.value.reslen/(element.value.duration*60)),
		}
		rows = append(rows, row)
	}
	err := save(outputPrefix+"_collection_prof.csv", rows)
	if err != nil {
		return err
	}
	return nil
}
