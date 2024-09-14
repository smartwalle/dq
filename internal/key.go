package internal

import "fmt"

func QueueKey(qname string) string {
	return fmt.Sprintf("dq:{%s}", qname)
}

func ScheduleKey(qname string) string {
	return fmt.Sprintf("%s:schedule", QueueKey(qname))
}

func PendingKey(qname string) string {
	return fmt.Sprintf("%s:pending", QueueKey(qname))
}

func ActiveKey(qname string) string {
	return fmt.Sprintf("%s:active", QueueKey(qname))
}

func RetryKey(qname string) string {
	return fmt.Sprintf("%s:retry", QueueKey(qname))
}

func ConsumerKey(qname string) string {
	return fmt.Sprintf("%s:consumer", QueueKey(qname))
}

func MessageKeyPrefix(qname string) string {
	return fmt.Sprintf("%s:m:", QueueKey(qname))
}

func MessageKey(qname, id string) string {
	return fmt.Sprintf("%s%s", MessageKeyPrefix(qname), id)
}
