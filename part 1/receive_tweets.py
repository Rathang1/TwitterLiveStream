import requests
import os
import json
import socket

# bearer token used for twitter authentication
bearer_token = os.environ.get("BEARER_TOKEN")
def bearer_oauth(r):
    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2FilteredStreamPython"
    return r

# server socket connection which the spark 
def connect_socket():
    try:
        skt = socket.socket()
        host = "0.0.0.0"    
        port = 4444
        skt.bind((host, port))
        print('socket is ready')
        skt.listen(4)
        print('socket is listening')
        client, address = skt.accept()
        skt.close()
        print("Received request from: " + str(address))
        return client
    except socket.error as err:
        print(err)

# get twitter streams
def receive_stream():
    # get client socket which the twitter endpoint will connect to
    client = connect_socket()

    # fetching twitter streams from api endpoint
    res = requests.get(
        "https://api.twitter.com/2/tweets/search/stream", auth=bearer_oauth, stream=True,
    )
    print("res code for getting streams")
    print(res.status_code)
    if res.status_code != 200:
        raise Exception(
            "Cannot get stream (HTTP {}): {}".format(
                res.status_code, res.text
            )
        )
    
    # process received tweeet object and send the data over socket to spark
    for json_r in res.iter_lines():
        if json_r:
            # convert json object to json dictionary
            dict = json.loads(json_r, strict=False)
            
            # create new json object
            data = {}
            data["id"] = dict["data"]["id"]
            data["text"] = dict["data"]["text"]
            data["tag"] = dict["matching_rules"][0]["tag"]
            
            # print tweet objects to console
            print(json.dumps(data, indent=4, sort_keys=True))
            
            # send tweet object over socket
            client.send(str(data["text"]+"t_tag"+data["tag"]+"t_end").encode('utf-8'))

def main():
    receive_stream()

if __name__ == "__main__":
    main()