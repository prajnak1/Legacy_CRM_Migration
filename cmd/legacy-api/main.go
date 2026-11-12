package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
)

// The source intentionally mimics a legacy API: aliases, inconsistent casing,
// IDs of mixed type, and occasional malformed records.
var customers = []map[string]any{
	{"ID": 1, "FullName": "Ada Lovelace", "email_address": "ada@example.test", "Company": "Analytical Engines", "created": "2022-01-12"},
	{"id": "2", "name": "Grace Hopper", "Email": "grace@example.test", "company_name": "Navy", "created_at": "2022-02-03"},
	{"customer_id": 3, "NAME": "Katherine Johnson", "email": "katherine@example.test", "company": "NASA", "createdAt": "2022-03-18"},
	{"ID": 4, "FullName": "", "email_address": "missing-name@example.test", "Company": "Unknown"},
	{"id": "5", "name": "Alan Turing", "Email": "alan@example.test", "company_name": "Bletchley Park"},
}
var contacts = []map[string]any{
	{"ID": 101, "CustomerID": 1, "Name": "Charles Babbage", "email_address": "charles@example.test", "phone": "+44 20 0000 0001"},
	{"id": "102", "customer_id": "2", "full_name": "Robert W. Taylor", "Email": "robert@example.test", "Phone": "555-0102"},
	{"contact_id": 103, "account_id": 3, "name": "Dorothy Vaughan", "email": "dorothy@example.test"},
	{"ID": 104, "CustomerID": 5, "Name": "Alex Morgan", "email_address": "alex@example.test"},
}
var activities = []map[string]any{
	{"ID": 1001, "CustomerID": 1, "ContactID": 101, "Type": "CALL", "Subject": "Introductory call", "OccurredAt": "2023-01-04T10:30:00Z"},
	{"id": "1002", "customer_id": "2", "contact_id": "102", "activity_type": "email", "title": "Proposal sent", "date": "2023-02-10"},
	{"activity_id": 1003, "account_id": 3, "kind": "meeting", "subject": "Project kickoff", "occurred_at": "2023-03-02T15:00:00Z"},
	{"ID": 1004, "CustomerID": 1, "Type": "note", "Subject": "Account preferences recorded"},
}

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { json.NewEncoder(w).Encode(map[string]string{"status": "ok"}) })
	for name, rows := range map[string][]map[string]any{"customers": customers, "contacts": contacts, "activities": activities} {
		rows := rows
		mux.HandleFunc("/v1/"+name, func(w http.ResponseWriter, r *http.Request) {
			page, _ := strconv.Atoi(r.URL.Query().Get("page")); if page < 1 { page = 1 }
			limit, _ := strconv.Atoi(r.URL.Query().Get("limit")); if limit < 1 || limit > 100 { limit = 2 }
			start := (page-1)*limit; end := start+limit; if start > len(rows) { start = len(rows) }; if end > len(rows) { end = len(rows) }
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{"data": rows[start:end], "page": page, "limit": limit, "total": len(rows), "has_more": end < len(rows)})
		})
	}
	addr := ":8081"
	fmt.Println("legacy CRM mock API listening on", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}
