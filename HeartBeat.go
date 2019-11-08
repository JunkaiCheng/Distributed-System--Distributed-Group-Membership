package main

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var addrList = [10]string{"172.22.152.196", "172.22.154.192", "172.22.156.192", "172.22.152.197", "172.22.154.193", "172.22.156.193", "172.22.152.198", "172.22.154.194", "172.22.156.194", "172.22.152.199"}
var vm = make(map[string]int)

//INTRODUCER is the IP address for introducer
const INTRODUCER = "172.22.154.192"

//const INTRODUCER = "130.126.153.43"

var currIP string
var currIndex int
var membershipList []Membership
var mutex = &sync.Mutex{}
var startTime time.Time

//Membership is the struct to store the timestamp and IP of a member
type Membership struct {
	IP        string
	TimeStamp time.Time
}

//Message is the struct to store the info of an event (join, leave, crash or heartbeat)
type Message struct {
	IP   string
	Info int //0: heartbeat, 1: apply to join, 2: leave, 3: crash, 4: join
}

//MembershipList implement the Len Less Swap of MembershipList for Sort
type MembershipList []Membership

//Neighbour is the struct to store the IP and last info time
type Neighbour struct {
	IP       string
	lastTime time.Time
}

//SendList stores the three neighbours to send messages
var SendList [3]Neighbour

//ListenList stores the three neighbours to listen
var ListenList [3]Neighbour

func (I MembershipList) Len() int {
	return len(I)
}
func (I MembershipList) Less(i, j int) bool {
	return I[i].IP < I[j].IP
}
func (I MembershipList) Swap(i, j int) {
	I[i], I[j] = I[j], I[i]
}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

func initialize() {
	//set up
	addrs, err := net.InterfaceAddrs()
	checkError(err)
	for _, addrs := range addrs {
		if ipnet, ok := addrs.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				currIP = ipnet.IP.String()
			}
		}
	}
	startTime = time.Now()
	currIndex = 0
	newNeighbour := Neighbour{currIP, time.Now()}
	for i := 0; i < 3; i++ {
		ListenList[i] = newNeighbour
		SendList[i] = newNeighbour
	}
	for i := 0; i < 10; i++ {
		vm[addrList[i]] = i + 1
	}
	if _, err := os.Stat("./log.txt"); err == nil {
		err := os.Remove("./log.txt")
		checkError(err)
	}
}

func updateTimer(currTime time.Time) {
	startTime = currTime
}

func inMemberList(ip string) bool {
	//check whether the IP is in membership list
	for i := 0; i < len(membershipList); i++ {
		if ip == membershipList[i].IP {
			return true
		}
	}
	return false
}

func deleteMember(ip string) bool {
	// delete the member from membership list
	for i := 0; i < len(membershipList); i++ {
		if ip == membershipList[i].IP {
			membershipList = append(membershipList[:i], membershipList[i+1:]...)
			return true
		}
	}
	return false
}

func handleNeighbourClient(conn *net.UDPConn) {
	//handle the messages from neighbours
	var buf [1024]byte
	message := Message{}
	n, _, err := conn.ReadFromUDP(buf[0:])
	checkError(err)

	//decode the byte to message
	dec := gob.NewDecoder(bytes.NewReader(buf[:n]))
	dec.Decode(&message)
	currTime := time.Now()
	if message.IP == currIP {
		return
	}
	switch message.Info {
	case 0:
		//Heartbeat: still alive and without change
		mutex.Lock()
		//check whether it is in the list
		if !inMemberList(message.IP) {
			newMember := Membership{message.IP, currTime}
			membershipList = append(membershipList, newMember)
			sort.Sort(MembershipList(membershipList))
			updateIndex()
			updateNeighbour()
			newMessage := Message{message.IP, 4}
			sendToNeighbour(newMessage)
			updateLog(message.IP, "join", time.Now())
			//fmt.Println(vm[message.IP], ": "+message.IP+" join!")
		}
		updateNeighbourTime(message.IP)
		mutex.Unlock()

	case 1:
		//apply to join (only introducer will get this info)
		//Update the membership list & local file, send the list to the new VM
		newMember := Membership{message.IP, currTime}
		mutex.Lock()
		membershipList = append(membershipList, newMember)
		sort.Sort(MembershipList(membershipList))
		updateIndex()
		updateNeighbour()
		mutex.Unlock()
		go updateFile()
		sendMembershipList(message.IP)
		updateLog(message.IP, "join", time.Now())
		//fmt.Println(vm[message.IP], ": "+message.IP+" join!")

	case 2:
		//Leave
		mutex.Lock()
		//Check whether it is in the list
		//If so, delete it and send the info
		//If not, stop sending it
		if deleteMember(message.IP) {
			updateIndex()
			updateNeighbour()
			sendToNeighbour(message)
			updateLog(message.IP, "leave", time.Now())
			//fmt.Println(vm[message.IP], ": "+message.IP+" leave!")
		}
		mutex.Unlock()
		if currIP == INTRODUCER {
			go updateFile()
		}

	case 3:
		//Crash
		mutex.Lock()
		//Check whether it is in the list
		//If so, delete it and send the info
		//If not, stop sending it
		if deleteMember(message.IP) {
			updateIndex()
			updateNeighbour()
			sendToNeighbour(message)
			updateLog(message.IP, "crash", time.Now())
			//fmt.Println(vm[message.IP], ": "+message.IP+" crash!")
		}
		mutex.Unlock()
		if currIP == INTRODUCER {
			go updateFile()
		}

	case 4:
		//Join
		mutex.Lock()
		//check whether it is in the list
		if !inMemberList(message.IP) {
			newMember := Membership{message.IP, currTime}
			membershipList = append(membershipList, newMember)
			sort.Sort(MembershipList(membershipList))
			updateIndex()
			updateNeighbour()
			sendToNeighbour(message)
			updateLog(message.IP, "join", time.Now())
			//fmt.Println(vm[message.IP], ": "+message.IP+" join!")
		}
		updateNeighbourTime(message.IP)
		mutex.Unlock()
	}
}

func updateNeighbourTime(ip string) {
	//When receiving heartbeat, update the time in neighbour list
	for i := 0; i < 3; i++ {
		if ListenList[i].IP == ip {
			ListenList[i].lastTime = time.Now()
		}
	}
}

func updateLog(ip string, info string, t time.Time) {
	//update the local log file
	f := "log.txt"
	//check whether the file exists
	file, err := os.OpenFile(f, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	defer file.Close()
	if err != nil && os.IsNotExist(err) {
		file, err = os.Create(f)
	}
	_, err = file.Write([]byte("VM " + strconv.Itoa(vm[ip]) + " " + info + "! Time stamp: " + t.String() + "\n"))
	checkError(err)
}

func updateFile() {
	//update the local file
	file, err := os.Create("member_list.txt")
	checkError(err)
	defer file.Close()
	for i := 0; i < len(membershipList); i++ {
		file.Write([]byte(membershipList[i].IP + "," + membershipList[i].TimeStamp.String() + "\n"))
	}
}

func listenNeighbour() {
	//listen to the heartbeat or other messages from neighbours
	udpAddr, err := net.ResolveUDPAddr("udp", ":1111")
	checkError(err)
	conn, err := net.ListenUDP("udp", udpAddr)
	checkError(err)
	defer conn.Close()
	for {
		handleNeighbourClient(conn)
	}
}

func listenIntroducer() bool {
	//Wait for introducer sending back the membershipList
	//If the introducer is down, return false
	udpAddr, err := net.ResolveUDPAddr("udp", ":1116")
	checkError(err)
	conn, err := net.ListenUDP("udp", udpAddr)
	checkError(err)
	defer conn.Close()
	for {
		var buf [1024]byte
		var members MembershipList
		conn.SetDeadline(startTime.Add(5 * time.Second))
		n, _, err := conn.ReadFromUDP(buf[0:])
		if err != nil {
			return false
		}
		//decode the byte to membership list
		dec := gob.NewDecoder(bytes.NewReader(buf[:n]))
		dec.Decode(&members)
		currTime := time.Now()
		for j := 0; j < len(members); j++ {
			members[j].TimeStamp = currTime
		}
		membershipList = members
		updateIndex()
		updateNeighbour()
		fmt.Println("Jion!")
		return true
	}
}

func updateIndex() {
	//update the current index of this VM in membership list
	for i := 0; i < len(membershipList); i++ {
		if currIP == membershipList[i].IP {
			currIndex = i
		}
	}
}

func updateNeighbour() {
	//update the IP and time of this VM's neighbours
	l := len(membershipList)
	if l == 1 {
		for i := 0; i < 3; i++ {
			newNeighbour := Neighbour{currIP, time.Now()}
			ListenList[i] = newNeighbour
			SendList[i] = newNeighbour
		}
	}
	if l == 2 {
		index := (currIndex + 1) % l
		newNeighbour := Neighbour{membershipList[index].IP, time.Now()}
		ListenList[0] = newNeighbour
		SendList[0] = newNeighbour
		for i := 1; i < 3; i++ {
			newNeighbour := Neighbour{currIP, time.Now()}
			ListenList[i] = newNeighbour
			SendList[i] = newNeighbour
		}
	}
	if l == 3 {
		index1 := (currIndex + 1) % l
		newNeighbour1 := Neighbour{membershipList[index1].IP, time.Now()}
		ListenList[0] = newNeighbour1
		SendList[0] = newNeighbour1

		index2 := (currIndex + 2) % l
		newNeighbour2 := Neighbour{membershipList[index2].IP, time.Now()}
		ListenList[1] = newNeighbour2
		SendList[1] = newNeighbour2

		newNeighbour := Neighbour{currIP, time.Now()}
		ListenList[2] = newNeighbour
		SendList[2] = newNeighbour
	}
	//For listen list
	if l > 3 {
		var offset = [3]int{-3, -2, -1}
		var flagOld = [3]int{0, 0, 0}
		var flagNew = [3]int{0, 0, 0}
		for i := 0; i < 3; i++ {
			index := (currIndex + offset[i] + l) % l
			ip := membershipList[index].IP
			flag := 1
			for j := 0; j < 3; j++ {
				if ip == ListenList[j].IP {
					flagOld[j] = 1
					flag = 0
					break
				}
			}
			flagNew[i] = flag
		}
		for i := 0; i < 3; i++ {
			if flagOld[i] == 0 {
				for j := 0; j < 3; j++ {
					if flagNew[j] == 1 {
						index := (currIndex + offset[j] + l) % l
						ip := membershipList[index].IP
						newNeighbour := Neighbour{ip, time.Now()}
						ListenList[i] = newNeighbour
						flagNew[j] = 0
						break
					}
				}
			}
		}

		//for send list
		var offset2 = [3]int{1, 2, 3}
		var flagOld2 = [3]int{0, 0, 0}
		var flagNew2 = [3]int{0, 0, 0}
		for i := 0; i < 3; i++ {
			index := (currIndex + offset2[i]) % l
			ip := membershipList[index].IP
			flag := 1
			for j := 0; j < 3; j++ {
				if ip == SendList[j].IP {
					flagOld2[j] = 1
					flag = 0
					break
				}
			}
			flagNew2[i] = flag
		}
		for i := 0; i < 3; i++ {
			if flagOld2[i] == 0 {
				for j := 0; j < 3; j++ {
					if flagNew2[j] == 1 {
						index := (currIndex + offset2[j]) % l
						ip := membershipList[index].IP
						newNeighbour := Neighbour{ip, time.Now()}
						SendList[i] = newNeighbour
						flagNew2[j] = 0
						break
					}
				}
			}
		}
	}
}

func heartbeat() {
	//send heartbents to its neighbours
	for {
		message := Message{currIP, 0}
		sendHBToNeighbour(message)
		time.Sleep(500 * time.Millisecond)
	}
}

func sendHBToNeighbour(message Message) {
	//send messages to its neighbours
	for i := 0; i < len(SendList); i++ {
		sendHBMessage(message, SendList[i].IP)
	}
}

func sendHBMessage(message Message, IP string) {
	//send one message to the given IP

	//encode the message to byte
	if IP != currIP && IP != "" {
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		enc.Encode(message)
		currAddr, err := net.ResolveUDPAddr("udp", currIP+":1117")
		checkError(err)
		serverAddr, err := net.ResolveUDPAddr("udp", IP+":1111")
		checkError(err)
		conn, err := net.DialUDP("udp", currAddr, serverAddr)
		checkError(err)
		//------can add simulated loss here
		_, err = conn.Write(buf.Bytes())
		checkError(err)
		conn.Close()
	}
}

func checkCrash() {
	//check whether neighbour fails
	for {
		currTime := time.Now()
		for i := 0; i < 3; i++ {
			if ListenList[i].IP != currIP && currTime.Sub(ListenList[i].lastTime) > 4*time.Second {
				mutex.Lock()
				//fmt.Println("Detect", vm[ListenList[i].IP], ListenList[i].IP, "crash!")
				handleCrash(ListenList[i].IP)
				mutex.Unlock()
				if currIP == INTRODUCER {
					go updateFile()
				}
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func handleCrash(ip string) {
	//delete the ip from membershiplist
	//update index and neighbour list
	//send the message to neighbours
	if deleteMember(ip) {
		updateIndex()
		updateNeighbour()
		message := Message{ip, 3}
		sendToNeighbour(message)
	}
}

func sendToNeighbour(message Message) {
	//send messages to its neighbours
	for i := 0; i < len(SendList); i++ {
		sendMessage(message, SendList[i].IP)
	}
}

func sendJoin() {
	//send the join info to introducerß
	message := Message{currIP, 1}
	sendMessage(message, INTRODUCER)
}

func sendLeave() {
	//send the leave info to neighbours
	message := Message{currIP, 2}
	sendToNeighbour(message)
}

func sendMessage(message Message, IP string) {
	//send one message to the given IP

	//encode the message to byte
	if IP != currIP && IP != "" {
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		enc.Encode(message)
		currAddr, err := net.ResolveUDPAddr("udp", currIP+":1113")
		checkError(err)
		serverAddr, err := net.ResolveUDPAddr("udp", IP+":1111")
		checkError(err)
		conn, err := net.DialUDP("udp", currAddr, serverAddr)
		checkError(err)
		//------can add simulated loss here
		_, err = conn.Write(buf.Bytes())
		checkError(err)
		conn.Close()
	}
}

func sendMembershipList(IP string) {
	//send membership list to newly joined VM
	//for i := 0; i < 3; i++ {
	//encode the membershiplist to byte
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	enc.Encode(membershipList)
	currAddr, err := net.ResolveUDPAddr("udp", currIP+":1115")
	checkError(err)
	serverAddr, err := net.ResolveUDPAddr("udp", IP+":1116")
	checkError(err)
	conn, err := net.DialUDP("udp", currAddr, serverAddr)
	checkError(err)
	_, err = conn.Write(buf.Bytes())
	checkError(err)
	conn.Close()
	//time.Sleep(100 * time.Millisecond)
	//}
}

func main() {
	initialize()
	if currIP != INTRODUCER {
		//if it is not the introducer, send Join to the introducer
		sendJoin()
		//wait for introducer sending back the membershipList
		if !listenIntroducer() {
			fmt.Println("The introducer is down, ", currIP, " cannot join into the group.")
			return
		}
	} else {
		// if it is the introducer, add itself to the membership list
		currTime := time.Now()
		//if memberlist file not exsits, create a new group
		//if not, rejoin
		if _, err := os.Stat("./member_list.txt"); err != nil {
			newMember := Membership{currIP, currTime}
			membershipList = append(membershipList, newMember)
			updateLog(currIP, "join", time.Now())
		} else {
			file, err := os.Open("./member_list.txt")
			checkError(err)
			defer file.Close()

			br := bufio.NewReader(file)
			for {
				a, _, c := br.ReadLine()
				if c == io.EOF {
					break
				}
				line := strings.Split(string(a), ",")
				newMember := Membership{line[0], time.Now()}
				membershipList = append(membershipList, newMember)
			}
			updateLog(currIP, "join", time.Now())
			updateIndex()
			updateNeighbour()
			updateFile()
		}
	}
	//start sending heartbeats
	go heartbeat()
	//start listen to its neighbours
	go listenNeighbour()
	//check whether neighbour fails
	go checkCrash()
	//handle the input: voluntarily leave
	fmt.Println("Type 'q' to leave and press 'l' to show the current list")
	for {
		reader := bufio.NewReader(os.Stdin)
		text, _ := reader.ReadString('\n')
		if text == "q\n" {
			fmt.Println("See you!")
			sendLeave()
			if currIP == INTRODUCER {
				err := os.Remove("./member_list.txt")
				checkError(err)
			}
			return
		}
		if text == "l\n" {
			for i := 0; i < len(membershipList); i++ {
				fmt.Println("ID: ", vm[membershipList[i].IP], "  TimeStamp: ", membershipList[i].TimeStamp.String())
			}
		}
	}
}
