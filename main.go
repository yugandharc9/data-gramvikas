package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

func getChangedFiles() ([]string, error) {
	cmd := exec.Command("git", "diff", "--name-only", "HEAD~1", "HEAD")

	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out

	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("git diff failed: %s", out.String())
	}

	var files []string
	for _, f := range strings.Split(out.String(), "\n") {
		if strings.HasSuffix(f, ".yaml") || strings.HasSuffix(f, ".yml") {
			files = append(files, f)
		}
	}
	return files, nil
}

func main() {
	files, err := getChangedFiles()
	if err != nil {
		log.Println("err while getting files", err)
		return
	}

	host := os.Getenv("API_HOST")
	token := os.Getenv("API_TOKEN")
	if host == "" {
		log.Println("No API_HOST given")
		return
	}
	if token == "" {
		log.Println("No API_TOKEN given")
		return
	}
	// files := []string{
	// 	"data/jalgaon/yawal/atraval/en/about.yaml",
	// }

	if len(files) == 0 {
		log.Println("No YAML changes detected.")
		return
	}

	bulkData := make(map[string][]map[string]interface{}, 0)
	bulkReq := make(map[string]string, 0)

	for _, f := range files {
		yamlFile, err := os.ReadFile(f)
		if err != nil {
			log.Printf("could not read file - %s | err %s", f, err.Error())
			return
		}
		dynamicData := make(map[string]interface{})
		err = yaml.Unmarshal(yamlFile, &dynamicData)
		if err != nil {
			log.Printf("Problem unmarshalling YAML: %v", err)
			return
		}
		log.Println("Processing:", f)
		district, taluka, village, lang, resource, err := getIdsAndLangFromPath(f)
		if err != nil {
			log.Printf("error getting ids and lang - %s", err.Error())
			return
		}
		dynamicData["districtID"] = district
		dynamicData["talukaID"] = taluka
		dynamicData["langID"] = lang
		dynamicData["villageID"] = village
		id := fmt.Sprintf("%s-%s-%s-%s", district, taluka, lang, village)
		dynamicData["id"] = id
		request := map[string]interface{}{"data": dynamicData, "id": id}
		if dataArray, ok := bulkData[resource]; !ok {
			dtArr := make([]map[string]interface{}, 0)
			dtArr = append(dtArr, request)
			bulkData[resource] = dtArr
		} else {
			dataArray = append(dataArray, request)
			bulkData[resource] = dataArray
		}
	}

	for key, data := range bulkData {
		jsonBytes, err := json.Marshal(data)
		if err != nil {
			log.Printf("%s %s", key, err.Error())
			continue
		}
		bulkReq[key] = string(jsonBytes)
	}
	for resource, dataReq := range bulkReq {
		createData(resource, dataReq)
	}
}

func getIdsAndLangFromPath(path string) (district, taluka, village, lang, resource string, err error) {
	re := regexp.MustCompile(`^data\/([^\/]+)\/([^\/]+)\/([^\/]+)\/(en|mr)\/([^\/]+\.ya?ml)$`)
	matches := re.FindStringSubmatch(path)

	if matches == nil {
		err = errors.New("No match for: " + path)
		return
	}

	district = matches[1]
	taluka = matches[2]
	village = matches[3]
	lang = matches[4]
	resource = strings.ReplaceAll(strings.ReplaceAll(matches[5], ".yaml", ""), ".yaml", "")
	return
}

func createData(resourceName, jsonString string) {
	log.Println(jsonString)
	url := fmt.Sprintf("%s/api/v1/bulk/%s?upsert=true", os.Getenv("API_HOST"), resourceName)
	log.Println(url)
	method := "PUT"

	payload := strings.NewReader(jsonString)

	client := &http.Client{}
	req, err := http.NewRequest(method, url, payload)

	if err != nil {
		log.Printf("%s %s", resourceName, err.Error())
		return
	}
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", os.Getenv("API_TOKEN")))
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Cookie", "csrftoken=XcmV3edjQzbhsxM0WHuIOvHbFQ4Ib0SDhs5vq0bw3134252LFwTESnGveAovaztx")

	res, err := client.Do(req)
	if err != nil {
		log.Printf("%s %s", resourceName, err.Error())
		return
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		log.Printf("%s %s", resourceName, err.Error())
		return
	}
	if res.StatusCode != 200 {
		log.Printf("failed to create %s body %s", resourceName, string(body))
	}

}
