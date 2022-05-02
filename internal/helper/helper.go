package helper

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"time"

	kitlog "github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/go-redis/redis"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type covidDataResponse struct {
	State           string     `json:"state"`
	District        string     `json:"district"`
	StateCovidCases covidCases `json:"state_covid_cases"`
	IndiaCovidCases covidCases `json:"india_covid_cases"`
	LastUpdateTime  string     `json:"last_updated_time"`
}
type covidCases struct {
	Confirmed   int `json:"confirmed"`
	Deceased    int `json:"deceased"`
	Recovered   int `json:"recovered"`
	Tested      int `json:"tested"`
	Vaccinated1 int `json:"vaccinated1"`
	Vaccinated2 int `json:"vaccinated2"`
}

type CovidDataDocumentResponse struct {
	Total covidCases `json:"total"`
}
type data struct {
	Name       string `json:"name"`
	Region     string `json:"region"`
	Country    string `json:"country"`
	RegionCode string `json:"region_code"`
}

func UpdateStatesCovidData(logger kitlog.Logger, col *mongo.Collection) error {
	level.Info(logger).Log("msg", "inside get state wise covid data func")
	//api to get covid data
	resp, err := http.Get("https://data.covid19india.org/v4/min/data.min.json")
	if err != nil {
		return err
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewBuffer(body))
	var responseDetails interface{}
	decoder.Decode(&responseDetails)
	ctx, _ := context.WithTimeout(context.Background(), 15*time.Second)
	//delte all documents exist in container
	if err = col.Drop(ctx); err != nil {
		fmt.Println("delte ERROR:", err)
		return err
	}
	//insert in mongodb
	result, insertErr := col.InsertOne(ctx, responseDetails)
	if insertErr != nil {
		fmt.Println("InsertOne ERROR:", insertErr)
		return insertErr
	}
	level.Info(logger).Log("result", result)
	return nil
}
func GetCovidData(logger kitlog.Logger, coordinates string, col *mongo.Collection, redisClient *redis.Client) (*covidDataResponse, error) {
	level.Info(logger).Log("msg", "inside get state wise covid data func")
	var res covidDataResponse
	accessKey := "f339d48829e9a78e9c8e7484f91ee0c6"
	//get state based on geocoordinates
	resp, err := http.Get("http://api.positionstack.com/v1/reverse?access_key=" + accessKey + "&query=" + coordinates + "&limit=1")
	if err != nil {
		return nil, err
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	level.Info(logger).Log("body", string(body))
	decoder := json.NewDecoder(bytes.NewBuffer(body))
	responseDetails := struct {
		Data []data `json:"data"`
	}{}
	decoder.Decode(&responseDetails)
	level.Info(logger).Log("Response", responseDetails)
	responseData := responseDetails.Data[0]

	//Check Data in Cache
	val, err := redisClient.Get(responseData.RegionCode).Result()
	if err == nil {
		err = json.Unmarshal([]byte(val), &res)
		if err != nil {
			return nil, err
		}
		if val != "" {
			return &res, nil
		}
	}

	//get document from mongodb
	var podcast bson.M
	ctx, _ := context.WithTimeout(context.Background(), 15*time.Second)
	if err = col.FindOne(ctx, bson.M{}).Decode(&podcast); err != nil {
		level.Error(logger).Log("error", err)
		return nil, err
	}

	res.State = responseData.Region
	res.District = responseData.Name
	var totalCases covidCases

	for key, _ := range podcast {
		if key == "_id" {
			var id string
			valBits, _ := json.Marshal(podcast[key])
			err = json.Unmarshal(valBits, &id)
			if err != nil {
				return nil, err
			}
			log.Println("id", id)
			time1, _ := strconv.ParseInt(id[0:8], 16, 0)
			tm := time.Unix(time1, 0).Format("2006-01-02 15:04:05")
			res.LastUpdateTime = fmt.Sprint(tm)

		}

		var data CovidDataDocumentResponse
		valBits, _ := json.Marshal(podcast[key])
		err = json.Unmarshal(valBits, &data)
		if err != nil {
			continue
		}

		//total cases in india
		totalCases.Confirmed = totalCases.Confirmed + data.Total.Confirmed
		totalCases.Deceased = totalCases.Deceased + data.Total.Deceased
		totalCases.Recovered = totalCases.Recovered + data.Total.Recovered
		totalCases.Tested = totalCases.Tested + data.Total.Tested
		totalCases.Vaccinated1 = totalCases.Vaccinated1 + data.Total.Vaccinated1
		totalCases.Vaccinated2 = totalCases.Vaccinated2 + data.Total.Vaccinated2
		if key == responseData.RegionCode {
			res.StateCovidCases = data.Total
		}

	}
	res.IndiaCovidCases = totalCases
	//Cache Data for 30 minutes
	redisData, err := json.Marshal(res)
	if err != nil {
		return nil, err
	}
	err = redisClient.Set(responseData.RegionCode, redisData, 30*time.Minute).Err()
	if err != nil {
		return nil, err
	}
	return &res, nil
}
