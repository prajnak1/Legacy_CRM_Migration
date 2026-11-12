package migrate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Status struct {
	RunID string `json:"runId"`
	State string `json:"state"`
	StartedAt *time.Time `json:"startedAt"`
	FinishedAt *time.Time `json:"finishedAt"`
	Processed int `json:"processed"`
	Failed int `json:"failed"`
	Resource string `json:"resource"`
	Page int `json:"page"`
	Verification map[string]any `json:"verification,omitempty"`
	Error string `json:"error,omitempty"`
}

type Worker struct { DB *pgxpool.Pool; Source string; Client *http.Client; Batch int; Rate time.Duration }

func New(db *pgxpool.Pool, source string) *Worker { return &Worker{DB:db, Source:strings.TrimRight(source,"/"), Client:&http.Client{Timeout:15*time.Second}, Batch:2, Rate:80*time.Millisecond} }

func (w *Worker) Init(ctx context.Context) error {
	_, err := w.DB.Exec(ctx, `CREATE TABLE IF NOT EXISTS customers (id BIGINT PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL DEFAULT '', company TEXT NOT NULL DEFAULT '', created_at TIMESTAMPTZ, migrated_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS contacts (id BIGINT PRIMARY KEY, customer_id BIGINT NOT NULL REFERENCES customers(id), name TEXT NOT NULL, email TEXT NOT NULL DEFAULT '', phone TEXT NOT NULL DEFAULT '', migrated_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS activities (id BIGINT PRIMARY KEY, customer_id BIGINT NOT NULL REFERENCES customers(id), contact_id BIGINT REFERENCES contacts(id), kind TEXT NOT NULL, subject TEXT NOT NULL DEFAULT '', occurred_at TIMESTAMPTZ, migrated_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS migration_runs (id TEXT PRIMARY KEY, state TEXT NOT NULL, started_at TIMESTAMPTZ NOT NULL DEFAULT now(), finished_at TIMESTAMPTZ, processed INT NOT NULL DEFAULT 0, failed INT NOT NULL DEFAULT 0, resource TEXT NOT NULL DEFAULT '', page INT NOT NULL DEFAULT 0, verification JSONB, error TEXT NOT NULL DEFAULT '');
CREATE TABLE IF NOT EXISTS migration_checkpoints (run_id TEXT NOT NULL REFERENCES migration_runs(id), resource TEXT NOT NULL, page INT NOT NULL DEFAULT 0, PRIMARY KEY(run_id,resource));
CREATE TABLE IF NOT EXISTS dead_letters (id BIGSERIAL PRIMARY KEY, run_id TEXT NOT NULL, resource TEXT NOT NULL, source_id TEXT NOT NULL DEFAULT '', payload JSONB NOT NULL, error TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now());`)
	return err
}

func (w *Worker) Current(ctx context.Context) (Status,error) {
	var s Status
	var verify []byte
	err:=w.DB.QueryRow(ctx, `SELECT id,state,started_at,finished_at,processed,failed,resource,page,verification,error FROM migration_runs ORDER BY started_at DESC LIMIT 1`).Scan(&s.RunID,&s.State,&s.StartedAt,&s.FinishedAt,&s.Processed,&s.Failed,&s.Resource,&s.Page,&verify,&s.Error)
	if errors.Is(err,pgx.ErrNoRows) { return Status{State:"idle"},nil }; if err!=nil{return s,err}; if len(verify)>0 { _=json.Unmarshal(verify,&s.Verification) }; return s,nil
}

func (w *Worker) Run(ctx context.Context, resume bool) (string,error) {
	if err:=w.Init(ctx); err!=nil{return "",err}
	runID:=""
	if resume { _=w.DB.QueryRow(ctx, `SELECT id FROM migration_runs WHERE state IN ('running','failed') ORDER BY started_at DESC LIMIT 1`).Scan(&runID) }
	if runID=="" { runID=fmt.Sprintf("crm-%d",time.Now().UnixNano()); if _,err:=w.DB.Exec(ctx,`INSERT INTO migration_runs(id,state) VALUES($1,'running')`,runID);err!=nil{return "",err} } else { _,_=w.DB.Exec(ctx,`UPDATE migration_runs SET state='running',error='',finished_at=NULL WHERE id=$1`,runID) }
	for _,resource:=range []string{"customers","contacts","activities"} { if err:=w.migrateResource(ctx,runID,resource);err!=nil{_,_=w.DB.Exec(context.Background(),`UPDATE migration_runs SET state='failed',error=$2,finished_at=now() WHERE id=$1`,runID,err.Error());return runID,err} }
	verification,err:=w.verify(ctx); if err!=nil { _,_=w.DB.Exec(ctx,`UPDATE migration_runs SET state='failed',error=$2,finished_at=now() WHERE id=$1`,runID,err.Error());return runID,err }
	b,_:=json.Marshal(verification); _,err=w.DB.Exec(ctx,`UPDATE migration_runs SET state='completed',finished_at=now(),verification=$2 WHERE id=$1`,runID,b)
	return runID,err
}

type pageResponse struct { Data []map[string]any `json:"data"`; Total int `json:"total"`; HasMore bool `json:"has_more"` }
func (w *Worker) getPage(ctx context.Context, resource string, page int) (pageResponse,error) {
	var out pageResponse
	var last error
	for attempt:=0;attempt<4;attempt++ {
		if attempt>0 { select {case <-ctx.Done():return out,ctx.Err();case <-time.After(time.Duration(attempt*attempt)*150*time.Millisecond):} }
		req,err:=http.NewRequestWithContext(ctx,"GET",fmt.Sprintf("%s/v1/%s?page=%d&limit=%d",w.Source,resource,page,w.Batch),nil); if err!=nil{return out,err}
		resp,err:=w.Client.Do(req); if err!=nil {last=err;continue}; body,readErr:=io.ReadAll(resp.Body);resp.Body.Close(); if readErr!=nil {last=readErr;continue}; if resp.StatusCode==429||resp.StatusCode>=500 {last=fmt.Errorf("source returned %s",resp.Status);continue}; if resp.StatusCode>=400{return out,fmt.Errorf("source returned %s: %s",resp.Status,body)}
		if err:=json.NewDecoder(bytes.NewReader(body)).Decode(&out);err!=nil{return out,err}; return out,nil
	}
	return out,fmt.Errorf("source retries exhausted: %w",last)
}
func (w *Worker) migrateResource(ctx context.Context, runID, resource string) error {
	var page int; _=w.DB.QueryRow(ctx,`SELECT page FROM migration_checkpoints WHERE run_id=$1 AND resource=$2`,runID,resource).Scan(&page)
	for {
		out,err:=w.getPage(ctx,resource,page+1);if err!=nil{return err}
		tx,err:=w.DB.Begin(ctx);if err!=nil{return err}
		for _, row := range out.Data {
			id := value(row, "id", "customer_id", "contact_id", "activity_id")
			// A failed constraint statement aborts its transaction. Use a nested
			// transaction (PostgreSQL savepoint) so the batch can still log it.
			recordTx, beginErr := tx.Begin(ctx)
			err := beginErr
			if err == nil {
				err = w.writeRecord(ctx, recordTx, resource, row)
				if err == nil {
					err = recordTx.Commit(ctx)
				} else {
					_ = recordTx.Rollback(ctx)
				}
			}
			if err != nil {
				payload, _ := json.Marshal(row)
				_, dlErr := tx.Exec(ctx, `INSERT INTO dead_letters(run_id,resource,source_id,payload,error) VALUES($1,$2,$3,$4,$5)`, runID, resource, stringify(id), payload, err.Error())
				if dlErr != nil { _ = tx.Rollback(ctx); return dlErr }
				if _, dlErr = tx.Exec(ctx, `UPDATE migration_runs SET failed=failed+1 WHERE id=$1`, runID); dlErr != nil { _ = tx.Rollback(ctx); return dlErr }
			} else {
				if _, err = tx.Exec(ctx, `UPDATE migration_runs SET processed=processed+1,resource=$2,page=$3 WHERE id=$1`, runID, resource, page+1); err != nil { _ = tx.Rollback(ctx); return err }
			}
		}
		if _,err=tx.Exec(ctx,`INSERT INTO migration_checkpoints(run_id,resource,page) VALUES($1,$2,$3) ON CONFLICT(run_id,resource) DO UPDATE SET page=EXCLUDED.page`,runID,resource,page+1);err!=nil{tx.Rollback(ctx);return err}
		if err=tx.Commit(ctx);err!=nil{return err};page++
		if !out.HasMore {break}; select{case <-ctx.Done():return ctx.Err();case <-time.After(w.Rate):}
	}
	return nil
}
func normalizeKey(key string) string {
	key = strings.ToLower(key)
	return strings.NewReplacer("_", "", "-", "", " ", "").Replace(key)
}

func value(row map[string]any, keys ...string) any {
	for _, key := range keys {
		want := normalizeKey(key)
		for actual, v := range row {
			if normalizeKey(actual) == want && v != nil && strings.TrimSpace(fmt.Sprint(v)) != "" {
				return v
			}
		}
	}
	return nil
}
func stringify(v any) string {if v==nil{return ""};return fmt.Sprint(v)}
func idValue(v any) (int64,error) { if v==nil{return 0,fmt.Errorf("missing required identifier")}; return strconv.ParseInt(strings.TrimSpace(fmt.Sprint(v)),10,64) }
func dateValue(v any) any {if v==nil{return nil};s:=strings.TrimSpace(fmt.Sprint(v));for _,layout:=range []string{time.RFC3339,"2006-01-02","2006-01-02 15:04:05"}{if t,e:=time.Parse(layout,s);e==nil{return t}};return nil}
func (w *Worker) writeRecord(ctx context.Context, tx pgx.Tx, resource string,row map[string]any) error {
	var id int64; var err error
	switch resource {
	case "customers":
		id,err=idValue(value(row,"id","customer_id")); if err!=nil{return err}; name:=strings.TrimSpace(stringValue(value(row,"name","full_name")));if name==""{return fmt.Errorf("customer name is required")}; email:=strings.ToLower(strings.TrimSpace(stringValue(value(row,"email","email_address")))); if email!=""&&!strings.Contains(email,"@"){return fmt.Errorf("invalid email %q",email)}
		_,err=tx.Exec(ctx,`INSERT INTO customers(id,name,email,company,created_at) VALUES($1,$2,$3,$4,$5) ON CONFLICT(id) DO UPDATE SET name=EXCLUDED.name,email=EXCLUDED.email,company=EXCLUDED.company,created_at=COALESCE(EXCLUDED.created_at,customers.created_at),migrated_at=now()`,id,name,email,stringValue(value(row,"company","company_name")),dateValue(value(row,"created_at","created")))
	case "contacts":
		id,err=idValue(value(row,"id","contact_id"));if err!=nil{return err}; customerID,e:=idValue(value(row,"customer_id","account_id"));if e!=nil{return e};name:=strings.TrimSpace(stringValue(value(row,"name","full_name")));if name==""{return fmt.Errorf("contact name is required")};email:=strings.ToLower(strings.TrimSpace(stringValue(value(row,"email","email_address"))))
		_,err=tx.Exec(ctx,`INSERT INTO contacts(id,customer_id,name,email,phone) VALUES($1,$2,$3,$4,$5) ON CONFLICT(id) DO UPDATE SET customer_id=EXCLUDED.customer_id,name=EXCLUDED.name,email=EXCLUDED.email,phone=EXCLUDED.phone,migrated_at=now()`,id,customerID,name,email,stringValue(value(row,"phone")))
	case "activities":
		id,err=idValue(value(row,"id","activity_id"));if err!=nil{return err};customerID,e:=idValue(value(row,"customer_id","account_id"));if e!=nil{return e};var contact any;if v:=value(row,"contact_id");v!=nil{contact,e=idValue(v);if e!=nil{return e}};kind:=strings.ToLower(strings.TrimSpace(stringValue(value(row,"type","activity_type","kind"))));if kind==""{return fmt.Errorf("activity type is required")}
		_,err=tx.Exec(ctx,`INSERT INTO activities(id,customer_id,contact_id,kind,subject,occurred_at) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(id) DO UPDATE SET customer_id=EXCLUDED.customer_id,contact_id=EXCLUDED.contact_id,kind=EXCLUDED.kind,subject=EXCLUDED.subject,occurred_at=COALESCE(EXCLUDED.occurred_at,activities.occurred_at),migrated_at=now()`,id,customerID,contact,kind,stringValue(value(row,"subject","title")),dateValue(value(row,"occurred_at","occurredat","date")))
	default:return fmt.Errorf("unknown resource %s",resource)
	}; return err
}
func stringValue(v any) string {if v==nil{return ""};return fmt.Sprint(v)}

func (w *Worker) verify(ctx context.Context) (map[string]any,error) {
	out:=map[string]any{}; mismatches:=[]string{}
	for _,resource:=range []string{"customers","contacts","activities"}{var expected,actual int; page:=1;for{p,err:=w.getPage(ctx,resource,page);if err!=nil{return nil,err};expected=p.Total;if !p.HasMore{break};page++};if err:=w.DB.QueryRow(ctx,"SELECT count(*) FROM "+resource).Scan(&actual);err!=nil{return nil,err};out[resource]=map[string]int{"source":expected,"destination":actual,"difference":actual-expected};if expected!=actual{mismatches=append(mismatches,resource)}}
	out["passed"]=len(mismatches)==0;out["mismatches"]=mismatches;return out,nil
}
