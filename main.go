package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

type User struct {
	Name string
	Role string
}

func (u *User) listen(data string) {
	fmt.Printf("Notification to %s: %s\n", u.Name, data)
}

type Topic struct {
	Name        string
	Subscribers map[string]*User
	Messages    []Message
}

type Message struct {
	ID    string
	Topic string
	Text  string
}

var (
	users  = make(map[string]*User)
	topics = make(map[string]*Topic)
)

func addUser(name, role string) {
	if _, exists := users[name]; exists {
		fmt.Println("User already exists.")
		return
	}
	users[name] = &User{Name: name, Role: role}
	fmt.Printf("User %s with role %s added.\n", name, role)
}

func addTopic(topicName, adminName string) {
	admin, exists := users[adminName]
	if !exists || admin.Role != "ADMIN" {
		fmt.Println("Only ADMIN can add topics.")
		return
	}
	if _, exists := topics[topicName]; exists {
		fmt.Println("Topic already exists.")
		return
	}
	topics[topicName] = &Topic{
		Name:        topicName,
		Subscribers: make(map[string]*User),
		Messages:    []Message{},
	}
	fmt.Printf("Topic %s added by %s.\n", topicName, adminName)
}

func subscribeTopic(topicName, userName string) {
	topic, topicExists := topics[topicName]
	user, userExists := users[userName]
	if !topicExists {
		fmt.Println("Topic does not exist.")
		return
	}
	if !userExists {
		fmt.Println("User does not exist.")
		return
	}
	if _, subscribed := topic.Subscribers[userName]; subscribed {
		fmt.Println("User is already subscribed.")
		return
	}
	topic.Subscribers[userName] = user
	fmt.Printf("User %s subscribed to topic %s.\n", userName, topicName)
}

func publishMessage(messageBody string) {
	var msg Message
	err := json.Unmarshal([]byte(messageBody), &msg)
	if err != nil {
		fmt.Println("Invalid message format.")
		return
	}

	topic, exists := topics[msg.Topic]
	if !exists {
		fmt.Println("Topic does not exist.")
		return
	}

	topic.Messages = append(topic.Messages, msg)
	fmt.Printf("Message published to topic %s.\n", msg.Topic)
	processMessages(topic)
}

func processMessages(topic *Topic) {
	for _, msg := range topic.Messages {
		for _, subscriber := range topic.Subscribers {
			subscriber.listen(msg.Text)
		}
	}
	topic.Messages = []Message{}
}

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}
		input := scanner.Text()
		parts := strings.Fields(input)
		if len(parts) == 0 {
			continue
		}
		switch parts[0] {
		case "addUser":
			if len(parts) != 3 {
				fmt.Println("Usage: addUser userName role")
				continue
			}
			addUser(parts[1], parts[2])
		case "addTopic":
			if len(parts) != 3 {
				fmt.Println("Usage: addTopic topicName userName")
				continue
			}
			addTopic(parts[1], parts[2])
		case "subscribeTopic":
			if len(parts) != 3 {
				fmt.Println("Usage: subscribeTopic topicName userName")
				continue
			}
			subscribeTopic(parts[1], parts[2])
		case "publishMessage":
			if len(parts) < 2 {
				fmt.Println("Usage: publishMessage messageBody")
				continue
			}
			publishMessage(strings.Join(parts[1:], " "))
		case "exit":
			fmt.Println("Exiting...")
			return
		default:
			fmt.Println("Unknown command.")
		}
	}
}
