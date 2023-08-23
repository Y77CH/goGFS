# Script for deployment & benchmark

if [[ $1 = "-h" ]]
then
    echo "Script for setup and start service"
    echo "./setup.sh -s: run setup (install go & create directories)"
    echo "./setup.sh -d: clear the data directory (/var/gfs_data/)"
    echo "./setup.sh -ms: <hostname:port>: start master server with the given hostname"
    echo "./setup.sh -cs: <cs_hostname:port> <ms_hostname:port>: start chunkserver with given hostnames"
    echo "./setup.sh -wr <ms_hostname:port>: benchmarks writing"
    echo "./setup.sh -bs <cs_hostname:port>: baseline benchmarking"
    echo "./setup.sh -up <ms_hostname:port>: benchmarks file upload"
    exit 0
fi
if [[ $1 = "-s" ]]
then
    mkdir data
    mkdir ~/gfs_data/
    sudo mv ~/gfs_data/ /var/
    wget https://go.dev/dl/go1.20.7.linux-amd64.tar.gz -P ~/
    sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf ~/go1.20.7.linux-amd64.tar.gz
    exit 0
fi
if [[ $1 = "-d" ]]
then
    rm -f /var/gfs_data/*
    exit 0
fi

export PATH=$PATH:/usr/local/go/bin
if [[ $1 = "-ms" ]]
then
    go run server/*.go -addr $2 -type ms -dir '/var/gfs_data/'
    exit 0
fi
if [[ $1 = "-cs" ]]
then
    go run server/*.go -addr $2 -type cs -dir /var/gfs_data/ -master $3
    exit 0
fi
if [[ $1 = "-wr" ]]
then
    go run *.go -f $(geni-get client_id) -op w -ms $2
    go run *.go -f $(geni-get client_id) -op v -ms $2
    go run *.go -f $(geni-get client_id) -op lw -ms $2
    go run *.go -f $(geni-get client_id) -op v -ms $2
    exit 0
fi
if [[ $1 = "-bs" ]]
then
    go run *.go -op b -node $2
    exit 0
fi
if [[ $1 = "-up" ]]
then
    go run *.go -f data/ -op u -ms $2
    go run *.go -f data/ -op lu -ms $2
fi