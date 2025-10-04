package processor

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"sort"
	"stream_combination/models"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/tidwall/btree"
)

type EquiJoinPredicate struct {
	// This constructs two results, and then buckets those results. Items in the same bucket get emitted.
	Left  func(models.EventLike) string
	Right func(models.EventLike) string
}

func NewEquiJoin(left func(models.EventLike) string, right func(models.EventLike) string) *EquiJoinPredicate {
	return &EquiJoinPredicate{
		Left:  left,
		Right: right,
	}
}

type TimeBucket struct {
	timestamp time.Time
	// TODO: Change removal to using Tombstones and a background removal process.
	leftEvents  map[string]*btree.BTreeG[models.EventLike] // CorrelationKey => Events
	rightEvents map[string]*btree.BTreeG[models.EventLike] // CorrelationKey => Events
}

func (tb *TimeBucket) String() string { // Pointer receiver
	if tb == nil {
		return "TimeBucket{nil}"
	}

	leftSummary := make([]string, 0, len(tb.leftEvents))
	for key, tree := range tb.leftEvents {
		if tree != nil {
			leftSummary = append(leftSummary, fmt.Sprintf("%s:%d", key, tree.Len()))
		}
	}

	rightSummary := make([]string, 0, len(tb.rightEvents))
	for key, tree := range tb.rightEvents {
		if tree != nil {
			rightSummary = append(rightSummary, fmt.Sprintf("%s:%d", key, tree.Len()))
		}
	}

	return fmt.Sprintf("TimeBucket{%v: left=[%s], right=[%s]}",
		tb.timestamp.Format("15:04:05"),
		strings.Join(leftSummary, ", "),
		strings.Join(rightSummary, ", "))
}

type SlidingWindowJoin struct {
	id             uuid.UUID
	timeBuckets    []*TimeBucket
	windowDuration time.Duration
	bucketSize     time.Duration
	numBuckets     int
	equiJoinPreds  []EquiJoinPredicate
	resultsChan    chan models.EventLike
	bufferSize     int
}

func (swj *SlidingWindowJoin) slideWindowForward(newEventTime time.Time) {
	newBucketStart := newEventTime.Truncate(swj.bucketSize)

	for {
		lastBucket := swj.timeBuckets[len(swj.timeBuckets)-1]
		nextBucketStart := lastBucket.timestamp.Add(swj.bucketSize)

		if newBucketStart.Before(nextBucketStart) {
			break
		}

		newBucket := &TimeBucket{
			timestamp:   nextBucketStart,
			leftEvents:  make(map[string]*btree.BTreeG[models.EventLike]),
			rightEvents: make(map[string]*btree.BTreeG[models.EventLike]),
		}

		swj.timeBuckets = append(swj.timeBuckets, newBucket)
		swj.numBuckets++

		// Drop oldest bucket to maintain window size
		// TODO: Persist to disk here
		swj.timeBuckets = swj.timeBuckets[1:]
		swj.numBuckets--
	}
}

func (swj *SlidingWindowJoin) addFirstBucket(fromTime time.Time) {
	newBucket := &TimeBucket{
		timestamp:   fromTime.Truncate(swj.bucketSize),
		leftEvents:  make(map[string]*btree.BTreeG[models.EventLike]),
		rightEvents: make(map[string]*btree.BTreeG[models.EventLike]),
	}
	swj.timeBuckets = append(swj.timeBuckets, newBucket)
	swj.numBuckets++
}

func (swj *SlidingWindowJoin) AddLeft(ctx context.Context, event models.EventLike) error {
	return swj.addEvent(ctx, event, true)
}

func (swj *SlidingWindowJoin) AddRight(ctx context.Context, event models.EventLike) error {
	return swj.addEvent(ctx, event, false)
}

func (swj *SlidingWindowJoin) getCompositeKey(event models.EventLike, isLeft bool) string {
	// Extract composite key based on side
	joinPredicates := make([]string, len(swj.equiJoinPreds))
	for i, predicate := range swj.equiJoinPreds {
		if isLeft {
			joinPredicates[i] = url.QueryEscape(predicate.Left(event))
		} else {
			joinPredicates[i] = url.QueryEscape(predicate.Right(event))
		}
	}
	return strings.Join(joinPredicates, ":")
}

func (swj *SlidingWindowJoin) addEvent(ctx context.Context, event models.EventLike, isLeft bool) error {
	slog.Info("Added message to SlidingWindowJoin", "event", event, "isLeft", isLeft, "buckets", swj.timeBuckets)
	if len(swj.timeBuckets) == 0 {
		swj.addFirstBucket(event.GetTimestamp())
	}

	if matches := swj.findMatch(event, isLeft); len(matches) > 0 {
		slog.Info("Found match for event", "event", event, "isLeft", isLeft, "matches", matches)
		for _, match := range matches {
			var joinResult models.EventLike
			if isLeft {
				joinResult = models.NewJoinEvent(time.Now(), event, match)
			} else {
				joinResult = models.NewJoinEvent(time.Now(), match, event)
			}
			select {
			case swj.resultsChan <- joinResult:
			case <-ctx.Done():
				return ctx.Err()
			default:
				return fmt.Errorf("results channel full")
			}
		}

		return nil
	}

	newestBucket := swj.timeBuckets[len(swj.timeBuckets)-1]
	bucketEndTime := newestBucket.timestamp.Add(swj.bucketSize)

	if event.GetTimestamp().After(bucketEndTime) {
		swj.slideWindowForward(event.GetTimestamp())
	}

	eventBucket := sort.Search(swj.numBuckets, func(i int) bool {
		return swj.timeBuckets[i].timestamp.After(event.GetTimestamp())
	}) - 1

	if eventBucket < 0 {
		return fmt.Errorf("event too old: %v", event.GetTimestamp())
	}

	compositeKey := swj.getCompositeKey(event, isLeft)
	slog.Info("Adding event to SlidingWindowJoin", "compositeKey", compositeKey)

	// Get the correct events map
	var eventsMap map[string]*btree.BTreeG[models.EventLike]
	if isLeft {
		eventsMap = swj.timeBuckets[eventBucket].leftEvents
	} else {
		eventsMap = swj.timeBuckets[eventBucket].rightEvents
	}

	// Initialize tree if needed
	if eventsMap[compositeKey] == nil {
		eventsMap[compositeKey] = btree.NewBTreeG(func(a, b models.EventLike) bool {
			return a.GetTimestamp().Before(b.GetTimestamp())
		})
	}

	eventsMap[compositeKey].Set(event)
	return nil
}

// If there's a match for this event, remove them from the buckets, and return them.
func (swj *SlidingWindowJoin) findMatch(event models.EventLike, isLeft bool) []models.EventLike {
	matchedEvents := make([]models.EventLike, 0)
	toDelete := make([]models.EventLike, 0)

	earliestTime := event.GetTimestamp().Add(-swj.windowDuration)
	latestTime := event.GetTimestamp().Add(swj.windowDuration)
	compositeKey := swj.getCompositeKey(event, isLeft)
	startPivot := models.Event{Timestamp: earliestTime}

	for _, bucket := range swj.timeBuckets {
		if bucket.timestamp.Before(earliestTime) || bucket.timestamp.After(latestTime) {
			continue
		}

		var tree *btree.BTreeG[models.EventLike]
		var exists bool

		if isLeft {
			tree, exists = bucket.rightEvents[compositeKey]
		} else {
			tree, exists = bucket.leftEvents[compositeKey]
		}

		if !exists || tree == nil {
			continue // Skip if tree doesn't exist
		}

		tree.Ascend(startPivot, func(e models.EventLike) bool {
			if e.GetTimestamp().After(latestTime) {
				return false // Stop iteration
			}
			matchedEvents = append(matchedEvents, e)
			toDelete = append(toDelete, e) // Mark for deletion
			return true
		})

		for _, eventToDelete := range toDelete {
			tree.Delete(eventToDelete)
		}
		toDelete = toDelete[:0]
	}
	return matchedEvents
}

func NewSlidingWindowJoin(windowDuration time.Duration, equiJoinPreds []EquiJoinPredicate) *SlidingWindowJoin {
	bufferSize := 512 // Magic number - add to configuration
	bucketSize := calculateBucketSize(windowDuration)
	totalDuration := windowDuration + (windowDuration / 2)
	numBuckets := int(totalDuration / bucketSize)

	return &SlidingWindowJoin{
		id:             uuid.New(),
		timeBuckets:    make([]*TimeBucket, 0, numBuckets),
		windowDuration: windowDuration,
		bucketSize:     bucketSize,
		numBuckets:     numBuckets,
		equiJoinPreds:  equiJoinPreds,
		resultsChan:    make(chan models.EventLike, bufferSize),
		bufferSize:     bufferSize,
	}
}

func (swj *SlidingWindowJoin) ID() string {
	return swj.id.String()
}

func (swj *SlidingWindowJoin) Results(ctx context.Context, consumerID string, errorCh chan<- error) <-chan models.EventLike {
	return swj.resultsChan
}

func (swj *SlidingWindowJoin) Close() error { return nil }

func calculateBucketSize(window time.Duration) time.Duration {
	switch {
	case window <= 1*time.Hour:
		return 5 * time.Minute // 18 buckets for 1.5 hours
	case window <= 1*time.Hour*24:
		return 1 * time.Hour // 36 buckets for 1.5 days
	case window <= 7*time.Hour*24:
		return 6 * time.Hour // 28 buckets for 1.5 weeks
	default:
		// Aim for ~20-30 buckets
		return window / 20
	}
}
