package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	_ "github.com/anandnahadia/HandleCovidData/docs"
	"github.com/anandnahadia/HandleCovidData/internal/helper"
	kitlog "github.com/go-kit/log"
	"github.com/go-redis/redis"
	"github.com/labstack/echo/v4"
	"github.com/swaggo/echo-swagger"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

var col *mongo.Collection
var redisClient *redis.Client

// swagger:parameters CoordinatesInput
type CoordinatesInput struct {
	// Enter Geocoordinates Latitude,Longitude.
	// eg - 23.341310,72.578284
	GeoCoordinates string `json:"coordinates"`
}

// This is a user defined method to close resources.
// This method closes mongoDB connection and cancel context.
func close(client *mongo.Client, ctx context.Context,
	cancel context.CancelFunc) {

	// CancelFunc to cancel to context
	defer cancel()

	// client provides a method to close
	// a mongoDB connection.
	defer func() {

		// client.Disconnect method also has deadline.
		// returns error if any,
		if err := client.Disconnect(ctx); err != nil {
			panic(err)
		}
	}()
}

// This is a user defined method that accepts
// mongo.Client and context.Context
// This method used to ping the mongoDB, return error if any.
func ping(client *mongo.Client, ctx context.Context) error {

	// mongo.Client has Ping to ping mongoDB, deadline of
	// the Ping method will be determined by cxt
	// Ping method return error if any occurred, then
	// the error can be handled.
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		return err
	}
	fmt.Println("connected successfully")
	return nil
}

//initMongoDb starts mongodb connection
func initMongoDb() (*mongo.Client, context.Context, context.CancelFunc) {
	//start mongodb connection
	clientOptions := options.Client().
		ApplyURI("mongodb://anand:Anand1998@cluster0-shard-00-00.ayqy0.mongodb.net:27017,cluster0-shard-00-01.ayqy0.mongodb.net:27017,cluster0-shard-00-02.ayqy0.mongodb.net:27017/testing?ssl=true&replicaSet=atlas-y6q7zl-shard-0&authSource=admin&retryWrites=true&w=majority")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	// defer cancel()
	client, err := mongo.Connect(ctx, clientOptions)
	// client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		panic(err)
	}
	col = client.Database("testing").Collection("covidData")
	fmt.Println("Collection type:", reflect.TypeOf(col))

	// Ping mongoDB with Ping method
	ping(client, ctx)
	return client, ctx, cancel
}

//initRedis connects redis
func initRedis() {
	//Redis Server
	redisClient = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	pong, err := redisClient.Ping().Result()
	fmt.Println(pong, err)
	if err != nil {
		panic(err)
	}
}

// @title Echo Swagger Example API
// @version 1.0
// @description This is a sample server server.
// @termsOfService http://swagger.io/terms/

// @contact.name API Support
// @contact.url http://www.swagger.io/support
// @contact.email support@swagger.io

// @license.name Apache 2.0
// @license.url http://www.apache.org/licenses/LICENSE-2.0.html

// @host localhost:1323
// @BasePath /
// @schemes http
func main() {

	client, ctx, cancel := initMongoDb()
	// initRedis()
	// Release resource when the main
	// function is returned.
	defer close(client, ctx, cancel)
	e := echo.New()
	e.GET("/", echoSwagger.WrapHandler)
	//api to update covid cases in mongodb
	e.GET("/updateCovidCases", updateCovidCases)
	//api to get covid case of a state using geocoordinates
	e.GET("/covidData", covidData)
	//access swagger
	e.GET("/swagger/*", echoSwagger.WrapHandler)
	port := os.Getenv("PORT")
	e.Logger.Fatal(e.Start(port))

}

// @Summary update covid cases in mongodb.
// @Description update covid cases in mongodb.
// @Tags root
// @Accept */*
// @Produce json
// @Success 200 {object} map[string]interface{}
// @Router /updateCovidCases [get]
func updateCovidCases(c echo.Context) error {
	logger := kitlog.NewJSONLogger(kitlog.NewSyncWriter(os.Stdout))
	err := helper.UpdateStatesCovidData(logger, col)
	if err != nil {
		return c.String(404, err.Error())
	}
	return c.String(http.StatusOK, "Covid Data is Updated in Database")
}

// @Summary get state covid cases.
// @Description get state covid cases.
// @Tags root
// @Param input query CoordinatesInput true "No Comments"
// @Accept */*
// @Produce json
// @Success 200 {object} map[string]interface{}
// @Router /covidData [get]
func covidData(c echo.Context) error {
	logger := kitlog.NewJSONLogger(kitlog.NewSyncWriter(os.Stdout))
	coordinates := c.QueryParam("coordinates")
	if coordinates == "" {
		return c.String(400, "Enter geo coordinates")
	}
	coordinatesArray := strings.Split(coordinates, ",")
	if len(coordinatesArray) != 2 {
		return c.String(400, "Incorrect coordinates. Try 23.341310,72.578284")
	}
	for _, val := range coordinatesArray {
		if _, err := strconv.ParseFloat(val, 64); err != nil {
			return c.String(400, "Incorrect coordinates. Try 23.341310,72.578284")
		}
	}
	covidDataResponse, err := helper.GetCovidData(logger, coordinates, col, redisClient)
	if err != nil {
		return c.String(400, err.Error())
	}
	return c.JSON(http.StatusOK, covidDataResponse)
}
