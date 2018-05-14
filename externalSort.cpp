#include <iostream>
#include <cmath>
#include <thread>
#include <vector>
#include <fstream>
#include <chrono>
#include <queue>
#include <cassert>
#include <atomic>

#define NTHREADS 2
#define KB 1024L
#define MB (1024*KB)
#define GB (1024*MB)
#define KEY_SIZE 10
#define REC_SIZE 100
//#define TOT_REC 107374184 // 10 GB
#define TOT_REC 10737419 // 1 GB
// #define TOT_REC 10000
// TOT_REC  MUST be a integer multiple RECS_IN_MEM
#define RECS_IN_MEM (TOT_REC/(NTHREADS*10))

//using namespace std;
using std::thread;
using std::cout;
using std::string;
using std::vector;
using std::pair;
using std::make_pair;

void
merge(std::vector<std::string> &arr, int startA, int endA, int startB, int endB, std::vector<std::string> &sortedArr);

void mergeSort(std::vector<std::string> &arr, int start, int end, std::vector<std::string> &sortedArr);

void nWayMerge(string &partSortFile, unsigned long runSize, unsigned long remRecords);
std::atomic<unsigned long> readRecs {0};
std::atomic<unsigned long> writeRecs {0};


class HandleIO {

private:
    std::fstream fileStream;
    std::ios_base::openmode _mode;
    string fileName;

    void _setData(vector<string> &buffer, unsigned long buffStart, unsigned long startLine, unsigned long numLines){
        fileStream.clear();
        fileStream.seekp(startLine * REC_SIZE, std::ios::beg);
        for (int i = 0; i < numLines; ++i) {
            fileStream << buffer[buffStart + i] + "\n";
        }
        if (fileStream.fail() || fileStream.bad()){
            throw std::runtime_error("Write Failed");
        }
        fileStream.flush();
    }

    void _getData(vector<string> &buffer, unsigned long buffStart, unsigned long startLine, unsigned long numLines){
        fileStream.seekg(startLine*REC_SIZE, std::ios::beg);
        for (int i = 0; i < numLines; ++i) {
            if(!getline(fileStream, buffer[buffStart + i])){
                throw std::runtime_error("File doesn't have said number of lines");
            }
        }
    }
public:
    explicit HandleIO(const string &fileName, std::ios_base::openmode mode = std::ios::in | std::ios::out) {
        this->fileName = fileName;
        this->_mode = mode;
        fileStream.open(this->fileName, mode);
        if (!this->fileStream.is_open()) {
            throw std::runtime_error("File open Failed\n");
        }
    }

    void getData(vector<string> &buffer, unsigned long startLineNum, unsigned long numberOfLines) {


        fileStream.seekg(startLineNum * REC_SIZE, std::ios::beg);
        for (int i = 0; i < numberOfLines; ++i) {
            if (!getline(fileStream, buffer[i])) {
                throw std::runtime_error("File does not have said number of lines");
            }
        }
/*
        unsigned int IOThreads = NTHREADS;
        unsigned long linesPerThread = numberOfLines/(IOThreads);
#pragma omp parallel for num_threads(IOThreads)
        for (int i = 0; i < IOThreads; ++i) {
            HandleIO threadIO(this->fileName);
            threadIO._setData(buffer, i*linesPerThread, startLineNum + i*linesPerThread,linesPerThread);
        }
        unsigned long remLines = numberOfLines - (linesPerThread * IOThreads);
        if (remLines > 0){
            _getData(buffer, IOThreads*linesPerThread, startLineNum + IOThreads*linesPerThread, remLines);
        }
*/
        readRecs += numberOfLines;
    }


    void setData(vector<string> &buffer, unsigned long startLineNum, unsigned long numberOfLines) {

        unsigned long linesPerThread = numberOfLines/(NTHREADS);
#pragma omp parallel for num_threads(NTHREADS)
        for (int i = 0; i < NTHREADS; ++i) {
            HandleIO threadIO(this->fileName);
            threadIO._setData(buffer, i*linesPerThread, startLineNum + i*linesPerThread, linesPerThread);
        }
        unsigned long remLines = numberOfLines - (linesPerThread * NTHREADS);
        if (remLines > 0){
            _setData(buffer, NTHREADS*linesPerThread, startLineNum + NTHREADS*linesPerThread, remLines);
        }
        writeRecs += numberOfLines;
/*
        fileStream.clear();
        fileStream.seekp(startLineNum * REC_SIZE, std::ios::beg);
        for (int i = 0; i < numberOfLines; ++i) {
            fileStream << buffer[i] + "\n";
        }
        if (fileStream.fail() || fileStream.bad()) {
            throw std::runtime_error("Write Failed.!");
        }
        fileStream.flush();
*/
    }

    ~HandleIO() {
        fileStream.close();
    }
};

class Block {
private:
    std::unique_ptr<HandleIO> file_; // Handle to deal with I/O
    unsigned long numRecords;   // Number of records allowed in Block.
    unsigned long numCurrRecs;  // Number of records that are currenlty in block
    unsigned long startPos;     // The record number in the file where the block starts.
    unsigned long _recsFetched; // The total number of records fetched.
    unsigned long _runSize;     // The runSize (number of records) of the run from which the block holds records.
    bool stopRead;              // Set this to true when all the records from the corresponding sortedRun are read.
    vector<string> data;        // Data in the block.
public:
    explicit Block(string &fileName, unsigned long numRecords, unsigned long startPos, unsigned long runSize,
                   std::ios_base::openmode mode = std::ios::in | std::ios::out) {
        try {
            file_ = std::make_unique<HandleIO>(fileName, mode);
        }
        catch (...) {
            throw;
        }
        this->numRecords = numRecords;
        this->startPos = startPos;
        this->data.resize(numRecords);
        this->_runSize = runSize;
        this->_recsFetched = 0;
        this->numCurrRecs = 0;
        this->stopRead = false;
    }

    Block() = default;

    unsigned long getCurrRecsInBlock() {
        return numCurrRecs;
    }

    bool isReadComplete() {
        return this->stopRead;
    }

    void writeBlock(unsigned long startPos = 0, unsigned long numRecords = 0) {

        if (numRecords == 0)
            numRecords = this->numRecords;
        if (startPos == 0)
            startPos = this->startPos;

        try {
            file_->setData(data, startPos, numRecords);
        }
        catch (...) {
            throw;
        }
    }

    void readBlock(unsigned long startPos = 0, unsigned long numRecords = 0) {

        if (numRecords == 0)
            numRecords = this->numRecords;
        if (startPos == 0)
            startPos = this->startPos;

        bool flag = false;
        if (_recsFetched + numRecords > _runSize) {
            numRecords = _runSize - _recsFetched;
            assert(numRecords >= 0);
            flag = true;
            //cout << "Tried to fetch more records than that exits. Exiting..";
            //exit(-1);
        }

        try {
            if (!stopRead) {
                file_->getData(data, startPos, numRecords);

                if (flag)
                    stopRead = true;
            }
        }
        catch (...) {
            throw;
        }
        numCurrRecs = numRecords;
        _recsFetched += numRecords;
    }

    void readNext() {
        this->startPos = this->startPos + this->numCurrRecs;
        this->readBlock(this->startPos);
    }

    void setData(vector<string> data_){
        this->data = data_;
    }

    string &operator[](const unsigned long &ix) {
        try {
            return this->data.at(ix);
        }
        catch (...) {
            cout << "Exception.!";
            throw;
        }

    }
};

void sortedRuns(string &file, unsigned long startRecNum, unsigned long numRec);

void sortedRuns(string &fileName, unsigned long startRecNum, unsigned long numRec) {
    std::vector<std::string> arr(numRec);
    HandleIO *file_;
    try {
        file_ = new HandleIO(fileName);
        file_->getData(arr, startRecNum, numRec);
    }
    catch (std::exception &exp) {
        cout << exp.what();
        exit(-1);
    }
    auto sortedArr = std::vector<std::string>(arr);

    std::cout << arr.size() << std::endl;
    mergeSort(arr, 0, arr.size() - 1, sortedArr);

    try {
        file_->setData(sortedArr, startRecNum, numRec);
    }
    catch (std::exception &exp) {
        cout << exp.what();
        exit(-1);
    }
    delete file_;
}

int main() {

    string fileName;
    fileName = "/tmp/oneGBInp";
    //fileName = "/tmp/test.txt";
    HandleIO *file_;
    try {
        file_ = new HandleIO(fileName);
    }
    catch (std::exception &exp) {
        cout << exp.what();
        exit(-1);
    }

    unsigned long numRuns = TOT_REC / RECS_IN_MEM;
    unsigned long remRecs = TOT_REC - (RECS_IN_MEM * numRuns);

    cout << numRuns << std::endl;
    auto start = std::chrono::high_resolution_clock::now();
/*
    remRecs = TOT_REC - ((TOT_REC / RECS_IN_MEM) * RECS_IN_MEM);

    for (int j = 0; j < TOT_REC / RECS_IN_MEM; ++j) {
        remRecs = remRecs + RECS_IN_MEM - (runSize * NTHREADS);
        for (int i = 0; i < NTHREADS; ++i) {
            threads[i] = thread(sortedRuns, std::ref(*file_), i * runSize, runSize);
        }

        for (auto &thread_ : threads) {
            if (thread_.joinable())
                thread_.join();
        }
    }

    if (remRecs > RECS_IN_MEM) {
        cout << "Not implemented";
        exit(-1);

    } else if (remRecs > 1) {

        sortedRuns(*file_, TOT_REC - remRecs, remRecs);
    }*/

#pragma omp parallel for num_threads(NTHREADS)
    for (unsigned int i = 0; i < numRuns; ++i) {
        sortedRuns(fileName, i * RECS_IN_MEM, RECS_IN_MEM);
    }

    if (remRecs) // Needed in-case of 1 record.!
        numRuns += 1;

    if (remRecs > 1) {
        sortedRuns(fileName, (numRuns - 1) * RECS_IN_MEM, remRecs);
    }
    auto end_runs = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> diff_runs = end_runs - start;
    cout << "Elapsed time(Sec): " << diff_runs.count() << "\n";

    nWayMerge(fileName, numRuns, remRecs);

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> diff = end - start;
    cout << "Elapsed time(Sec): " << diff.count() << "\n";

    cout << "Number of 100 byte recs read from disk:" << readRecs << "\n";
    cout << "Number of 100 byte recs write to disk:" << writeRecs << "\n";

    delete file_;
    return 0;
}


void mergeSort(std::vector<std::string> &arr, int start, int end, std::vector<std::string> &sortedArr) {
    int arrLen = end - start + 1;
    if (arrLen <= 1) return;
    auto partition = static_cast<int>(std::ceil((start + end) / 2.00) - 1);

    //thread t1(mergeSort, std::ref(sortedArr), start, partition, std::ref(arr));
    mergeSort(sortedArr, start, partition, arr);
    mergeSort(sortedArr, partition + 1, end, arr);
    //if (t1.joinable())
      //  t1.join();
    merge(arr, start, partition, partition + 1, end, sortedArr);
}


void
merge(std::vector<std::string> &arr, int startA, int endA, int startB, int endB, std::vector<std::string> &sortedArr) {
    int i = startA; // Pointer to arr A
    int j = startB; // Pointer to arr B

    int k = i;

    // Move pointers, depending on which is large.
    while (i <= endA && j <= endB) {
        if (arr[i].compare(0, KEY_SIZE, arr[j]) > 0) {
            // write arr[j] to output
            sortedArr[k] = (arr[j]);
            k++;
            j++;
        } else if (arr[i].compare(0, KEY_SIZE, arr[j]) < 0) {
            // write arr[i] to output
            sortedArr[k] = (arr[i]);
            k++;
            i++;
        }
    }

    // Deal with cases where one of array has higher lengths.

    while (i <= endA) {
        sortedArr[k] = (arr[i]);
        k++;
        i++;
    }
    while (j <= endB) {
        sortedArr[k] = (arr[j]);
        k++;
        j++;
    }

}

void nWayMerge(string &partSortFile, unsigned long numRuns, unsigned long remRecords) {

    HandleIO *file_;
    try {
        file_ = new HandleIO(partSortFile);
    }
    catch (std::exception &exp) {
        cout << exp.what();
        exit(-1);
    }

    /* The total available RAM is divided into blocks.
     * Number of blocks = Number of sorted runs + 1; The +1 is needed to store the results of the Merge.
     * Each block can hold a maximum of x = floor(RECS_IN_MEM / numBlocks) records.
     * We load each sorted run in to a block, merge the blocks in to 1.
     * Note that we cannot load the entire run in to a block, we just load x records process them and read the remaining.
     * The same goes with the merge result. As the block gets filled we write the results back to disk.
     * */

    // Set "numBlocks" to number of sorted runs in the record
    auto numBlocks = numRuns;

    auto recsInBlock = static_cast<unsigned long>(floor(RECS_IN_MEM * NTHREADS / (numBlocks + 1)));

    vector<Block> blocks(numBlocks);
    //string resultFile = "/home/pvkc/Downloads/gensort-linux-1.5/64/tenGBSorted";
    string resultFile;
    resultFile = "/tmp/oneGBSorted";
    //resultFile = "/tmp/sortOpt";
    Block result(resultFile, recsInBlock, 0, TOT_REC, std::ios::out);

    unsigned int n = 0;
//#pragma omp parallel for num_threads(NTHREADS)
    for (n = 0; n < numBlocks - 1; ++n) {
        try {

            blocks[n] = Block(partSortFile, recsInBlock, n * RECS_IN_MEM, RECS_IN_MEM);
        }
        catch (std::exception &exp) {
            cout << exp.what();
            exit(-1);
        }

        blocks[n].readBlock();
    }

    if (remRecords > 0) {
        blocks[numBlocks - 1] = Block(partSortFile, recsInBlock, n * RECS_IN_MEM, remRecords);
    }
    else{
        blocks[numBlocks -1] = Block(partSortFile, recsInBlock, n* RECS_IN_MEM, RECS_IN_MEM);
    }
    blocks[numBlocks-1].readBlock();


    auto cmpFunc = [](const pair<string, pair<int, int> > &str1, const pair<string, pair<int, int> > &str2) {
        return str1.first > str2.first;
    };

    // "String", "Block number", "Record number in block"
    std::priority_queue<pair<string, int>, vector<pair<string, pair<int, int> >>, decltype(cmpFunc)> q(cmpFunc);

    unsigned long i = 0, m = 0;
    int j = 0, k = 0;

    // Push top element in each sorted run to min heap
    for (int l = 0; l < numBlocks; ++l) {
        q.push(make_pair(blocks[l][0], std::make_pair(l, 0)));
    }
    thread t1;
    vector<string> res(recsInBlock);
    while (!q.empty()) {
        for (m = 0; m < recsInBlock; ++m) {
            if (q.empty()) break;
            int blkNum = q.top().second.first;
            int recNum = q.top().second.second;
            // Extract the min from heap and store in buffer.
            res[m] = q.top().first;
            q.pop();
            // push a new record to the queue
            if (recNum + 1 < blocks[blkNum].getCurrRecsInBlock())
                q.push(make_pair(blocks[blkNum][recNum + 1], make_pair(blkNum, recNum + 1)));
            else {
                // If block is empty, read from disk until the end of sorted run.
                if (!blocks[blkNum].isReadComplete()) {
                    cout << blkNum << "," << recNum << "," << i + m << "\n";
                    blocks[blkNum].readNext();
                    if (blocks[blkNum].getCurrRecsInBlock() > 0)
                        q.push(make_pair(blocks[blkNum][0], make_pair(blkNum, 0)));
                }
            }
        }
        if (t1.joinable()){
            t1.join();
        }
        result.setData(res);
        t1 = thread(&Block::writeBlock, &result, i, m);
        i = i + m;
    }
    if (t1.joinable())
        t1.join();
    delete file_;
}



