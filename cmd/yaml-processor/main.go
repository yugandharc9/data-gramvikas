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

func main() {
	host := os.Getenv("API_HOST")
	token := os.Getenv("API_TOKEN")

	if host == "" {
		log.Fatal("API_HOST not set")
	}
	if token == "" {
		log.Fatal("API_TOKEN not set")
	}

	files, err := getChangedYAMLFiles()
	if err != nil {
		log.Fatal(err)
	}

	if len(files) == 0 {
		log.Println("No YAML changes detected")
		return
	}

	bulkData := make(map[string][]map[string]interface{})
	hasFailure := false

	for _, file := range files {
		if err := processFile(file, bulkData); err != nil {
			log.Println(err)
			hasFailure = true
		}
	}

	for resource, data := range bulkData {
		if err := sendBulk(resource, data, host, token); err != nil {
			log.Println(err)
			hasFailure = true
		}
	}

	if hasFailure {
		log.Fatal("one or more resources failed")
	}
}

func getChangedYAMLFiles() ([]string, error) {
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

func processFile(path string, bulk map[string][]map[string]interface{}) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read failed %s: %w", path, err)
	}

	var data map[string]interface{}
	if err := yaml.Unmarshal(raw, &data); err != nil {
		return fmt.Errorf("yaml parse failed %s: %w", path, err)
	}

	district, taluka, village, lang, resource, err := parsePath(path)
	if err != nil {
		return err
	}

	id := fmt.Sprintf("%s-%s-%s-%s", district, taluka, lang, village)

	data["districtID"] = district
	data["talukaID"] = taluka
	data["villageID"] = village
	data["langID"] = lang
	data["id"] = id

	payload := map[string]interface{}{
		"id":   id,
		"data": data,
	}

	bulk[resource] = append(bulk[resource], payload)

	log.Println("Prepared:", path)
	return nil
}

func sendBulk(resource string, data []map[string]interface{}, host, token string) error {
	body, err := json.Marshal(data)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("%s/api/v1/bulk/%s?upsert=true", host, resource)
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(body))
	if err != nil {
		return err
	}

	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	resp, _ := io.ReadAll(res.Body)
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("resource %s failed: %s", resource, string(resp))
	}

	log.Println("Uploaded:", resource)
	return nil
}

func parsePath(path string) (district, taluka, village, lang, resource string, err error) {
	re := regexp.MustCompile(`^data\/([^\/]+)\/([^\/]+)\/([^\/]+)\/(en|mr)\/([^\/]+)\.ya?ml$`)
	m := re.FindStringSubmatch(path)

	if m == nil {
		err = errors.New("invalid path: " + path)
		return
	}

	district = m[1]
	taluka = m[2]
	village = m[3]
	lang = m[4]
	resource = m[5]
	return
}
