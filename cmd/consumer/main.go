// Sleep Deprivation Detector
package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/spf13/pflag"
)

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
	brokers     = pflag.StringSliceP("brokers", "b", []string{"broker:9092", "broker:29092"}, "kafka brokers list")
	topic       = pflag.StringP("topic", "t", "students", "kafka topic name")
	deprivation = pflag.Float32P("deprivation", "d", 6.0, "sleep deprivation level")
	partitions  = pflag.IntP("partitionы", "p", 20, "kafka partitions")
	messages    = pflag.IntP("messages", "m", 15000, "expected number of messages")
)

func detectSleepDeprivation(msg *sarama.ConsumerMessage) (bool, error) {
	student := Student{}
	if err := json.Unmarshal(msg.Value, &student); err != nil {
		return false, fmt.Errorf("failed to unmarshal student: %w", err)
	}

	if student.SleepHours <= *deprivation {
		return true, nil
	}

	return false, nil
}

func main() {
	pflag.Parse()

	// Wait for kafka to start
	time.Sleep(15 * time.Second)

	slog.Info("sleep deprivation detector started", "p", *partitions, "gomaxprocs", runtime.GOMAXPROCS(0))
	start := time.Now()

	master, err := sarama.NewConsumer(*brokers, nil)
	if err != nil {
		slog.Error("failed to create new consumer", "err", err)
		os.Exit(1)
	}

	defer func() {
		if err := master.Close(); err != nil {
			slog.Error("failed to close consumer", "err", err)
		}
	}()

	consumeDone := make(chan struct{})
	consumedCond := sync.NewCond(&sync.Mutex{})

	go func() {
		defer close(consumeDone)
		consumedCond.L.Lock()
		defer consumedCond.L.Unlock()

		for *messages > 0 {
			consumedCond.Wait()
		}
	}()

	wg := sync.WaitGroup{}
	wg.Add(*partitions)

	for i := 0; i < *partitions; i++ {
		go func(partition int32) {
			defer wg.Done()

			consumer, err := master.ConsumePartition(*topic, partition, sarama.OffsetOldest)
			if err != nil {
				slog.Error("failed to create partition consumer", "err", err)
				os.Exit(1)
			}

			defer func() {
				if err := consumer.Close(); err != nil {
					slog.Error("failed to close partition consumer", "err", err)
				}
			}()

		Consumer:
			for {
				select {
				case msg := <-consumer.Messages():
					consumedCond.L.Lock()
					*messages--
					consumedCond.L.Unlock()
					consumedCond.Signal()

					deprivated, err := detectSleepDeprivation(msg)
					if err != nil {
						slog.Error("failed to detect sleep deprivation", "err", err)
						continue
					}

					if deprivated {
						slog.Warn("student may feels bad")
					} else {
						slog.Info("student got enough sleep")
					}
				case <-consumeDone:
					break Consumer
				}
			}
		}(int32(i))
	}

	wg.Wait()

	slog.Info("all students are processed", "elapsed", time.Since(start))
}
