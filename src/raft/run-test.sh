clear; time go test -run 2C -race | grep -i -C5 -E "fail|error"