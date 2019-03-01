// Copyright 2018 Google Inc. All Rights Reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"net"
	"net/http"
	"os"

	"cloud.google.com/go/spanner"
	"contrib.go.opencensus.io/exporter/stackdriver"
	"contrib.go.opencensus.io/exporter/stackdriver/propagation"
	"go.opencensus.io/plugin/ochttp"
	"go.opencensus.io/trace"
	"go.opencensus.io/zpages"
)

var (
	database        string
	project         string
	spannerClient   *spanner.Client
	defaultDatabase = "projects/hightowerlabs/instances/north-america/databases/weather"
)

type Weather struct {
	Location    string
	Temperature int64
}

func main() {
	flag.StringVar(&database, "database", defaultDatabase, "The database connection string")
	flag.StringVar(&project, "project", "hightowerlabs", "The GCP project ID")
	flag.Parse()

	httpListenPort := os.Getenv("PORT")
	if httpListenPort == "" {
		httpListenPort = "8080"
	}

	hostPort := net.JoinHostPort("0.0.0.0", httpListenPort)

	log.Println("Starting weather-api ...")

	stackdriverExporter, err := stackdriver.NewExporter(stackdriver.Options{ProjectID: project})
	if err != nil {
		log.Fatal(err)
	}

	trace.RegisterExporter(stackdriverExporter)
	trace.ApplyConfig(trace.Config{DefaultSampler: trace.AlwaysSample()})

	ctx := context.Background()

	spannerClient, err = spanner.NewClient(ctx, database)
	if err != nil {
		log.Fatal(err)
	}
	defer spannerClient.Close()

	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	zpages.Handle(nil, "/debug")
	http.HandleFunc("/api", weatherHandler)
	log.Printf("Listening on %s", hostPort)

	log.Fatal(http.ListenAndServe(hostPort, &ochttp.Handler{
		Propagation:    &propagation.HTTPFormat{},
		FormatSpanName: func(r *http.Request) string { return "weather-api" },
	}))
}

func weatherHandler(w http.ResponseWriter, r *http.Request) {
	log.Println("New weather API request...")

	w.Header().Set("Access-Control-Allow-Origin", "*")

	location := r.FormValue("location")
	if location == "" {
		log.Println("empty location parameter")
		http.Error(w, "missing location query parameter", http.StatusBadRequest)
		return
	}

	_, span := trace.StartSpan(r.Context(), "weather-database")
	span.AddAttributes(
		trace.StringAttribute("spanner", database),
	)

	span.Annotate([]trace.Attribute{
		trace.StringAttribute("Query", "SELECT location,temperature FROM weather WHERE location = $1"),
	}, "sql query")

	defer span.End()

	weather, err := getWeatherForLocation(r.Context(), location)
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := json.NewEncoder(w).Encode(weather); err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func getWeatherForLocation(ctx context.Context, location string) (*Weather, error) {
	var w Weather

	row, err := spannerClient.Single().ReadRow(ctx, "Weather",
		spanner.Key{location},
		[]string{"Location", "Temperature"},
	)

	if err != nil {
		return nil, err
	}

	if err := row.ToStruct(&w); err != nil {
		return nil, err
	}

	return &w, nil
}
