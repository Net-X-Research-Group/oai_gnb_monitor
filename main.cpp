#include <fstream>
#include <iostream>
#include <string>
#include <regex>
#include <map>
#include <thread>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <queue>

/* Example Frame Slot format
 *
 *   [NR_MAC]   Frame.Slot 128.0
 *   UE RNTI 928c CU-UE-ID 1 in-sync PH 45 dB PCMAX 21 dBm, average RSRP -83 (17 meas)
 *   UE 928c: CQI 13, RI 2, PMI (0,0)
 *   UE 928c: UL-RI 1, TPMI 0
 *   UE 928c: dlsch_rounds 681/10/1/0, dlsch_errors 0, pucch0_DTX 9, BLER 0.02678 MCS (1) 22
 *   UE 928c: ulsch_rounds 1136/77/0/0, ulsch_errors 0, ulsch_DTX 0, BLER 0.07390 MCS (1) 6 (Qm 4 deltaMCS 0 dB) NPRB 106  SNR 17.5 dB
 *   UE 928c: MAC:    TX         344885 RX        2627890 bytes
 *   UE 928c: LCID 1: TX            369 RX           1074 bytes
 *   UE 928c: LCID 2: TX              3 RX             25 bytes
 *   UE 928c: LCID 4: TX          43621 RX        2616709 bytes
 *   UE RNTI 6542 CU-UE-ID 1 in-sync PH 45 dB PCMAX 21 dBm, average RSRP -83 (17 meas)
 */


struct UEData{
    std::string rnti; // UE ID
    std::string state; // In-sync or Out-of-sync
    double rsrp{}; // Reference Signals Received Power (DOWNLINK)
    int pcmax{}; // Maximum UL Channel Transmit Power (dBm)
    int ue_id{}; // User Equipment ID
    int ph{}; // Power Headroom
    int cqi{}; // Channel Quality Index
    int dl_ri{}; // Downlink Rank Indicator
    int ul_ri{}; // Uplink Rank Indicator
    int dlsch_err{}; // Downlink Scheduling Errors
    int pucch_dtx{}; // PUCCH Discontinuous Transmission
    double dl_bler{}; // Downlink Block Error Rate
    int dl_mcs{}; // Downlink MCS
    int ulsch_err{}; // Uplink Scheduling Errors
    int ulsch_dtx{}; // Uplink Scheduling Discontinuous Transmissions
    double ul_bler{}; // Uplink Block Error Rate
    int ul_mcs{}; // Uplink MCS
    int nprb{}; // Number of PRB
    double snr{}; // Signal to Noise
    time_t timestamp{}; // Timestamp
};

template <typename T>
class SafeQueue {
private:
    std::queue<T> queue;
    mutable std::mutex mtx;
    std::condition_variable cv;
    bool done;

public:
    SafeQueue() : done(false) {}

    void push(T value) {
        std::lock_guard lock(mtx);
        queue.push(std::move(value));
        cv.notify_one();
    }

    bool attempt_pop(T& value) {
        std::lock_guard lock(mtx);
        if (queue.empty()) {
            return false;
        }
        value = std::move(queue.front());
        queue.pop();
        return true;
    }

    void wait_pop(T& value) {
        std::unique_lock lock(mtx);
        cv.wait(lock, [this]{return !queue.empty() || done;});
        if (done && queue.empty()) {
            return;
        }
        value = std::move(queue.front());
        queue.pop();
    }

    void set_done() {
        std::lock_guard lock(mtx);
        done = true;
        cv.notify_all();
    }

    bool is_empty() {
        std::lock_guard lock(mtx);
        return queue.empty();
    }
};


class Parser {
private:
    SafeQueue<std::string> line_queue;
    SafeQueue<UEData> data_queue;

    std::mutex combined_mtx;
    std::mutex ue_file_mtx;

    // Data Queue
    std::atomic<bool> done;


    std::map<std::string, std::ofstream> ue_file_handler;
    std::ofstream combined_file;
    bool export_combined;
    std::string filename;

    std::map<std::string, UEData> temp_ue_data;


    // Define regex patterns as class members to compile them once
    std::regex ue_basic_pattern;
    std::regex ue_indicators_1;
    std::regex ue_indicators_2;
    std::regex dl_phy;
    std::regex ul_phy;

public:
    explicit Parser(std::string file_name, const bool exportCombined = true) :
        export_combined(exportCombined),
        filename(std::move(file_name)),

        ue_basic_pattern(R"(UE RNTI (\w+) CU-UE-ID (\d+) (\w+-\w+) PH (\d+) dB PCMAX (\d+) dBm, average RSRP (-?\d+))"),
        ue_indicators_1(R"(UE (\w+): CQI (\d+), RI (\d+))"),
        ue_indicators_2(R"(UE (\w+): UL-RI (\d+))"),
        dl_phy(R"(UE (\w+):.+ dlsch_errors (\d+), pucch0_DTX (\d+), BLER ([0-9.]+) MCS .+ (\d+))"),
        ul_phy(R"(UE (\w+):.+ ulsch_errors (\d+), ulsch_DTX (\d+), BLER ([0-9.]+) MCS \(1\) (\d+) .+ NPRB (\d+)  SNR ([0-9.]+))")
    {
        done = false;
    }

    void parse_line(const std::string& line) {
        std::smatch matches;
        std::string rnti;

        if (std::regex_search(line, matches, ue_basic_pattern)) {
            rnti = matches[1];
            create_ue_data(rnti);
            temp_ue_data[rnti].timestamp = std::time(nullptr);
            temp_ue_data[rnti].ue_id = std::stoi(matches[2]);
            temp_ue_data[rnti].state = matches[3];
            temp_ue_data[rnti].ph = std::stoi(matches[4]);
            temp_ue_data[rnti].rsrp = std::stoi(matches[5]);
        }

        else if (std::regex_search(line, matches, ue_indicators_1)) {
            rnti = matches[1];
            temp_ue_data[rnti].cqi = std::stoi(matches[2]);
            temp_ue_data[rnti].dl_ri = std::stoi(matches[3]);
        }

        else if(std::regex_search(line, matches, ue_indicators_2)) {
            rnti = matches[1];
            temp_ue_data[rnti].ul_ri = std::stoi(matches[2]);
        }

        else if(std::regex_search(line, matches, dl_phy)) {
            rnti = matches[1];
            temp_ue_data[rnti].dlsch_err = std::stoi(matches[2]);
            temp_ue_data[rnti].pucch_dtx = std::stoi(matches[3]);
            temp_ue_data[rnti].dl_bler = std::stod(matches[4]);
            temp_ue_data[rnti].dl_mcs = std::stoi(matches[5]);
        }

        else if(std::regex_search(line, matches, ul_phy)) {
            rnti = matches[1];
            temp_ue_data[rnti].ulsch_err = std::stoi(matches[2]);
            temp_ue_data[rnti].ulsch_dtx = std::stoi(matches[3]);
            temp_ue_data[rnti].ul_bler = std::stod(matches[4]);
            temp_ue_data[rnti].ul_mcs = std::stoi(matches[5]);

            temp_ue_data[rnti].nprb = std::stoi(matches[6]);
            temp_ue_data[rnti].snr = std::stod(matches[7]);
            //store_data(rnti);
            data_queue.push(temp_ue_data[rnti]);
        }
        else {;}
    }

    void reader_thread_func(std::istream& input) {
        std::string line;
        while (std::getline(input, line)) {
            if (!line.empty()) {
                line_queue.push(line);
            }
        }
        line_queue.set_done();
    }

    void parser_thread_func() {
        std::string line;
        while (true) {
            line_queue.wait_pop(line);
            if (line_queue.is_empty() && done) break;
            try {
                parse_line(line);
            } catch (const std::exception& e) {
                std::cerr << "Error parsing line: " << line << std::endl;
                std::cerr << "Exception: " << e.what() << std::endl;
            }
        }
        data_queue.set_done();
    }

    void writer_thread_func() {
        UEData data;
        while (true) {
            data_queue.wait_pop(data);
            if (done && data_queue.is_empty()) break;
            store_data(data);
        }
    }

    void store_data(const UEData& data) {
        if (export_combined) {
            std::lock_guard lock(combined_mtx);
            char timeBuffer[300];
            strftime(timeBuffer, sizeof(timeBuffer), "%Y-%m-%d %H:%M:%S", localtime(&data.timestamp));
            combined_file << timeBuffer << ","
                    << data.rnti << ","
                    << data.ue_id << ","
                    << data.state << ","
                    << data.ph << ","
                    << data.pcmax << ","
                    << data.rsrp << ","
                    << data.cqi << ","
                    << data.dl_ri << ","
                    << data.ul_ri << ","
                    << data.dlsch_err << ","
                    << data.pucch_dtx << ","
                    << data.dl_bler << ","
                    << data.dl_mcs << ","
                    << data.ulsch_err << ","
                    << data.ulsch_dtx << ","
                    << data.ul_bler << ","
                    << data.ul_mcs << ","
                    << data.nprb << ","
                    << data.snr << std::endl;
        }

        if (!export_combined) {
            std::lock_guard lock(ue_file_mtx);
            // If the file handler doesn't exist yet, create it
            if (ue_file_handler.find(data.rnti) == ue_file_handler.end()) {
                const std::string ueFile = filename + "_" + data.rnti + ".csv";
                ue_file_handler[data.rnti].open(ueFile, std::ios::binary);
                std::vector<char> buffer(1024*1024);
                ue_file_handler[data.rnti].rdbuf()->pubsetbuf(buffer.data(), buffer.size());

                // Write header to the new file
                ue_file_handler[data.rnti] << "timestamp,rnti,ue_id,state,ph,pcmax,rsrp,cqi,dl_ri,ul_ri,"
                            << "dlsch_err,pucch_dtx,dl_bler,dl_mcs,ulsch_err,ulsch_dtx,"
                            << "ul_bler,ul_mcs,nprb,snr" << std::endl;
            }

            // Write the data
            char timeBuffer[30];
            strftime(timeBuffer, sizeof(timeBuffer), "%Y-%m-%d %H:%M:%S", localtime(&data.timestamp));

            ue_file_handler[data.rnti] << timeBuffer << ","
                    << data.rnti << ","
                    << data.ue_id << ","
                    << data.state << ","
                    << data.ph << ","
                    << data.pcmax << ","
                    << data.rsrp << ","
                    << data.cqi << ","
                    << data.dl_ri << ","
                    << data.ul_ri << ","
                    << data.dlsch_err << ","
                    << data.pucch_dtx << ","
                    << data.dl_bler << ","
                    << data.dl_mcs << ","
                    << data.ulsch_err << ","
                    << data.ulsch_dtx << ","
                    << data.ul_bler << ","
                    << data.ul_mcs << ","
                    << data.nprb << ","
                    << data.snr << std::endl;
        }

        // Only clear this RNTI's data
        temp_ue_data.erase(data.rnti);
    }

    void create_ue_data(const std::string& rnti) {
        if (temp_ue_data.find(rnti) == temp_ue_data.end()) {
            temp_ue_data[rnti] = UEData();
            temp_ue_data[rnti].rnti = rnti;
            temp_ue_data[rnti].timestamp = std::time(nullptr);
        }
    }
};


int main(const int argc, char* argv[]) {
    std::string line;
    const std::string outputFile = "ue_metrics";
    bool exportCombined = true;

    for (int i = 1; i < argc; i++) {
        if (std::string arg = argv[i]; arg == "--sep") {
            exportCombined = false;
        }
    }

    //freopen("gnb_fed3.log", "r", stdin); // FOR TESTING
    Parser parser(outputFile, exportCombined);

    std::thread reader(&Parser::reader_thread_func, &parser, std::ref(std::cin));
    std::thread processor(&Parser::parser_thread_func, &parser);
    std::thread writer(&Parser::writer_thread_func, &parser);


    reader.join();
    processor.join();
    writer.join();

}
