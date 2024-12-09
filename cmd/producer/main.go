package main

import (
	"encoding/json"
	"log/slog"
	"math/rand"
	"os"
	"time"

	"github.com/IBM/sarama"
	"github.com/gocarina/gocsv"
	"github.com/spf13/pflag"
)

// students dataset:
// * CSV format
// golang producer
// * JSON format
// kafka broker
// * students topic
// golang consumer
// * Absense Detector - check if student absent (attendance status =absent)
// * Sleep Deprivation Detector - check if student is lack of sleep (sleep hours <=6)
//
// test with different batch sizes and latencies
//

type Student struct {
	StudentID        string  `csv:"Student ID" json:"student_id"`
	Date             string  `csv:"Date" json:"date"`
	ClassTime        string  `csv:"Class Time" json:"class_time"`
	AttendanceStatus string  `csv:"Attendance Status" json:"attendance_status"`
	StressLevel      float32 `csv:"Stress Level (GSR)" json:"stress_level"`
	SleepHours       float32 `csv:"Sleep Hours" json:"sleep_hours"`
	AnxietyLevel     int     `csv:"Anxiety Level" json:"anxiety_level"`
	MoodScore        int     `csv:"Mood Score" json:"mood_score"`
	RiskLevel        string  `csv:"Risk Level" json:"risk_level"`
}

var (
	brokers          = pflag.StringSliceP("brokers", "b", []string{"broker:9092", "broker:29092"}, "kafka brokers list")
	topic            = pflag.StringP("topic", "t", "students", "kafka topic name")
	studentsFilename = pflag.StringP("file", "f", "student_monnitoring_data.csv", "students CSV filename")
	seed             = pflag.Int64P("seed", "s", 0, "students slice shuffle seed")
)

func main() {
	pflag.Parse()

	// Wait for kafka to start
	time.Sleep(15 * time.Second)

	slog.Info("student producer started")

	studentsFile, err := os.Open(*studentsFilename)
	if err != nil {
		slog.Error("failed to open students file", "err", err)
		os.Exit(1)
	}

	students := []Student{}
	if err := gocsv.UnmarshalFile(studentsFile, &students); err != nil {
		slog.Error("failed to unmarshal students file", "err", err)
		os.Exit(1)
	}

	rand.Seed(*seed)
	rand.Shuffle(len(students), func(i, j int) { students[i], students[j] = students[j], students[i] })

	producer, err := sarama.NewSyncProducer(*brokers, nil)
	if err != nil {
		slog.Error("failed to create producer", "err", err)
		os.Exit(1)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			slog.Error("failed to close producer", "err", err)
		}
	}()

	start := time.Now()

	for _, student := range students {
		bytes, err := json.Marshal(student)
		if err != nil {
			slog.Error("failed to marshal student", "err", err)
			continue
		}

		msg := &sarama.ProducerMessage{
			Topic: *topic,
			Value: sarama.ByteEncoder(bytes),
		}

		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			slog.Error("failed to send student", "err", err)
			continue
		}

		slog.Info("student is sent", "topic", *topic, "partition", partition, "offset", offset)
	}

	slog.Info("all students are producer finished", "elapsed", time.Since(start))
}
