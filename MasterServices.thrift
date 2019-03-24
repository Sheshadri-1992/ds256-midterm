namespace java jsr.jsk.prpe.thrift

typedef i32 int32

struct DataNodeLocation {
	1: required int32 nodeid;
	2: required string ip;
	3: required int32 port;
}

struct OpenFileRequest {
	1: required int32 requesttype;
	2: required string filename;
	3: required int32 filesize;
	4: required double storagebudget;
}

struct OpenFileResponse {
	1: required int32 handle;
	2: required int32 status;
}

struct WriteBlockRequest {
	1: required int32 handle;
}

struct WriteBlockResponse {
	1: required int32 type;
	2: required int32 blocknum;
	3: required list<DataNodeLocation> locations; 
}

struct BlockLocation {
	1: required int32 type;
	2: required int32 blocknumber;
	3: required list<DataNodeLocation> locations;	
}

struct ReadBlockRequest {
	1: required string filename;
}

struct ReadBlockResponse {
	1: required list<BlockLocation> blocklocations;
	2: required int32 status; 
}

struct ListFileRequest {
	1: required int32 handle;
}

struct ListFileResponse {
	1: required list<string> filenames;
	2: required int32 status;
}

struct RegisterEdgeRequest {
	1: required int32 nodeId;
	2: required DataNodeLocation edgeLocation;
}

struct RegisterEdgeResponse {
	1: required int32 status;
}

struct CloseFileRequest {
	1: required i32 handle;
}

struct CloseFileResponse {
	1: required i32 status;
}

struct HeartBeatRequest {
	1: required int32 nodeId;
}

struct HeartBeatResponse {
	1: required int32 status;
}

struct BlockReportRequest {
	1: required int32 nodeId;
	2: required DataNodeLocation datanodelocation;
	3: required list<int32> blocknumbers;
}

struct BlockReportResponse {
	1: required int32 status;
}

service MasterService {
	
	OpenFileResponse openFile(1:OpenFileRequest req);
	
	//This would be called multiple times, number of times is equal to the number of blocks
	WriteBlockResponse requestBlocksToWrite(1:WriteBlockRequest writeBlockReq);

	ReadBlockResponse requestBlocksToRead(1:ReadBlockRequest readBlockReq);

	RegisterEdgeResponse registerEdge(1:RegisterEdgeRequest registerReq);

	ListFileResponse listFiles(1:ListFileRequest listFileReq);

	BlockReportResponse blockReport(1:BlockReportRequest blockReportReq)

	HeartBeatResponse heartbeat(1:HeartBeatRequest heartBeatReq);

	CloseFileResponse closeFile(1:CloseFileRequest req);

	/** Warning use this method with caution **/
	CloseFileResponse clearStructures();
}


