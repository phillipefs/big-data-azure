### Azure Stream Analytics Jobs



First define a input object, in this case we will use our eventhubs.


```sh
src-app-music-events-json
```

After this define the output object, we will use another event hubs and blob storage container.

```sh
# event hubs
enriched-top-rated-musics-json

# blob storage
music-processed-blob-events

```

And now we will create a query of top rated music based on popularity.

```sql
--blob storage
SELECT
    *
INTO
    [music-processed-blob-events]
FROM
    [src-app-music-events-json]
WHERE popularity > 30

--event hubs
SELECT
    *
INTO
    [enriched-top-rated-musics-json]
FROM
    [src-app-music-events-json]
WHERE popularity > 30
```