# table

Buffer `database/sql` query results into an in-memory table.

```
t, err := table.NewBuffer(ctx, db, "select ID, Name from Account;")
if err != nil {
	return err
}
for _, row := range t.Rows {
	id := row.Get("ID").(int64)
	name := row.Get("Name").(string)
	// ...
}
```
