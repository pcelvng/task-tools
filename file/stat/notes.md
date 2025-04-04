 # Update stats.Stats to be thread safe without mutex 
 - Move string and bools to atomic.Value 
    - checkSum (string) 
    - path (string)
    - created (string)
    - isDir (string) 

file/buf -> no changes 
file/local -> completed, needs testing
file/minio -> completed, needs testing
file/nop -> y
