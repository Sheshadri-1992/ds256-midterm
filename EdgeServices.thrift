namespace java jsr.jsk.prpe.thrift

typedef i32 int32

struct PutRequest {
	1: required string blocknumber;
	2: required binary data;
}

struct PutResponse {
	1: required int32 response;
}

struct GetRequest {
	1: required int32 blockNumber;
}

struct GetResponse {
	1: required string blockNumber;
	2: required binary data;
	3: required int32 status;
}

service EdgeService {
	PutResponse put(1:PutRequest putReq);
	GetResponse get(1:GetRequest getReq);
}
