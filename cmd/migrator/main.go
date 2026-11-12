package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/example/legacy-crm-migrator/internal/migrate"
)

func main() {
	if len(os.Args)>1 && os.Args[1]=="migrate" { fs:=flag.NewFlagSet("migrate",flag.ExitOnError);resume:=fs.Bool("resume",false,"resume a previous incomplete run");_ = fs.Parse(os.Args[2:]);w,pool:=worker();defer pool.Close();if _,err:=w.Run(context.Background(),*resume);err!=nil{log.Fatal(err)};return }
	if len(os.Args)>1 && os.Args[1]=="serve" { serve();return }
	log.Fatal("usage: migrator serve | migrator migrate [--resume]")
}
func worker() (*migrate.Worker,*pgxpool.Pool) {
	url:=env("DATABASE_URL","postgres://crm:crm@localhost:5432/crm?sslmode=disable");ctx:=context.Background();pool,err:=pgxpool.New(ctx,url);if err!=nil{log.Fatal(err)};return migrate.New(pool,env("LEGACY_API_URL","http://localhost:8081")),pool
}
func env(key, fallback string) string {if v:=os.Getenv(key);v!=""{return v};return fallback}
func serve() {
	w,pool:=worker();defer pool.Close();ctx:=context.Background();if err:=w.Init(ctx);err!=nil{log.Fatal(err)}
	var mu sync.Mutex;running:=false
	mux:=http.NewServeMux()
	mux.HandleFunc("/healthz",func(rw http.ResponseWriter,_ *http.Request){rw.Header().Set("Content-Type","application/json");rw.Write([]byte(`{"status":"ok"}`))})
	mux.HandleFunc("/api/status",func(rw http.ResponseWriter,r *http.Request){s,err:=w.Current(r.Context());if err!=nil{http.Error(rw,err.Error(),500);return};var dlq int;_ = pool.QueryRow(r.Context(),"SELECT count(*) FROM dead_letters WHERE run_id=$1",s.RunID).Scan(&dlq);data:=map[string]any{"run":s,"deadLetters":dlq};writeJSON(rw,data)})
	mux.HandleFunc("/api/migrations",func(rw http.ResponseWriter,r *http.Request){if r.Method==http.MethodGet{rows,err:=pool.Query(r.Context(),`SELECT id,state,started_at,finished_at,processed,failed,resource,page,verification,error FROM migration_runs ORDER BY started_at DESC LIMIT 10`);if err!=nil{http.Error(rw,err.Error(),500);return};defer rows.Close();items:=[]migrate.Status{};for rows.Next(){var s migrate.Status;var raw []byte;if rows.Scan(&s.RunID,&s.State,&s.StartedAt,&s.FinishedAt,&s.Processed,&s.Failed,&s.Resource,&s.Page,&raw,&s.Error)==nil{if len(raw)>0{_ = json.Unmarshal(raw,&s.Verification)};items=append(items,s)}};writeJSON(rw,items);return};if r.Method!=http.MethodPost{http.Error(rw,"method not allowed",405);return};var body struct{Resume bool `json:"resume"`};_ = json.NewDecoder(r.Body).Decode(&body);mu.Lock();if running{mu.Unlock();http.Error(rw,"a migration is already running",409);return};running=true;mu.Unlock();go func(){defer func(){mu.Lock();running=false;mu.Unlock()}();if _,err:=w.Run(context.Background(),body.Resume);err!=nil{log.Printf("migration failed: %v",err)}}();writeJSON(rw,map[string]any{"accepted":true,"resume":body.Resume})})
	mux.HandleFunc("/api/dead-letters",func(rw http.ResponseWriter,r *http.Request){rows,err:=pool.Query(r.Context(),`SELECT id,run_id,resource,source_id,payload,error,created_at FROM dead_letters ORDER BY created_at DESC LIMIT 100`);if err!=nil{http.Error(rw,err.Error(),500);return};defer rows.Close();type item struct{ID int64 `json:"id"`;RunID string `json:"runId"`;Resource string `json:"resource"`;SourceID string `json:"sourceId"`;Payload json.RawMessage `json:"payload"`;Error string `json:"error"`;CreatedAt time.Time `json:"createdAt"`};all:=[]item{};for rows.Next(){var x item;if rows.Scan(&x.ID,&x.RunID,&x.Resource,&x.SourceID,&x.Payload,&x.Error,&x.CreatedAt)==nil{all=append(all,x)}};writeJSON(rw,all)})
	addr:=":"+env("PORT","8080");log.Println("migration control API listening on",addr);log.Fatal(http.ListenAndServe(addr,cors(mux)))
}
func writeJSON(w http.ResponseWriter,v any){w.Header().Set("Content-Type","application/json");_ = json.NewEncoder(w).Encode(v)}
func cors(next http.Handler) http.Handler{return http.HandlerFunc(func(w http.ResponseWriter,r *http.Request){w.Header().Set("Access-Control-Allow-Origin","http://localhost:4200");w.Header().Set("Access-Control-Allow-Headers","Content-Type");w.Header().Set("Access-Control-Allow-Methods","GET,POST,OPTIONS");if r.Method==http.MethodOptions{w.WriteHeader(204);return};next.ServeHTTP(w,r)})}

