


### Clearing Cache

`gharchive-metrics` has aggressive caching in order to speed up subsequent queries. When
there are bugs this cache becomes problematic and needs to be cleared.

```
aws s3 rm s3://ipfs-metrics/cache --recursive  
```

