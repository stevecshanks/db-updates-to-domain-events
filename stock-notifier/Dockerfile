FROM golang:1.17

WORKDIR /go/src/stock-notifier

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN go build

CMD ["./stock-notifier"]
