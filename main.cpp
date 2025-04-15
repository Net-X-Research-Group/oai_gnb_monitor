#include <fstream>
#include <iostream>
#include <string>
#include <regex>
#include <map>



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
    double rsrp; // Reference Signals Received Power (DOWNLINK)
    int pcmax; // Maximum UL Channel Transmit Power (dBm)
    int ue_id; // User Equipment ID
    int ph; // Power Headroom
    int cqi; // Channel Quality Index
    int dl_ri; // Downlink Rank Indicator
    int ul_ri; // Uplink Rank Indicator
    int dlsch_err; // Downlink Scheduling Errors
    int pucch_dtx; // PUCCH Discontinuous Transmission
    double dl_bler; // Downlink Block Error Rate
    int dl_mcs; // Downlink MCS
    int ulsch_err; // Uplink Scheduling Errors
    int ulsch_dtx; // Uplink Scheduling Discontinuous Transmissions
    double ul_bler; // Uplink Block Error Rate
    int ul_mcs; // Uplink MCS
    int nprb; // Number of PRB
    double snr; // Signal to Noise
    time_t timestamp; // Timestamp
};

class Parser {
private:
    std::map<std::string, std::vector<UEData>> ue_data_by_rnti;
    std::map<std::string, UEData> temp_ue_data;

    // Define regex patterns as class members to compile them once
    std::regex ue_basic_pattern;
    std::regex ue_indicators_1;
    std::regex ue_indicators_2;
    std::regex dl_phy;
    std::regex ul_phy;

public:
    Parser() :
            ue_basic_pattern(R"(UE RNTI (\w+) CU-UE-ID (\d+) (\w+-\w+) PH (\d+) dB PCMAX (\d+) dBm, average RSRP (-?\d+))"),
            ue_indicators_1(R"(UE (\w+): CQI (\d+), RI (\d+))"),
            ue_indicators_2(R"(UE (\w+): UL-RI (\d+))"),
            dl_phy(R"(UE (\w+):.+ dlsch_errors (\d+), pucch0_DTX (\d+), BLER ([0-9.]+) MCS .+ (\d+))"),
            ul_phy(R"(UE (\w+):.+ ulsch_errors (\d+), ulsch_DTX (\d+), BLER ([0-9.]+) MCS \(1\) (\d+) .+ NPRB (\d+)  SNR ([0-9.]+))")
    {}

    void create_ue_data(const std::string& rnti) {
        if (temp_ue_data.find(rnti) == temp_ue_data.end()) {
            temp_ue_data[rnti] = UEData();
            temp_ue_data[rnti].rnti = rnti;
            temp_ue_data[rnti].timestamp = std::time(nullptr);
        }
    }

    void store_data(const std::string& rnti) {
        UEData& data = temp_ue_data[rnti];
        ue_data_by_rnti[rnti].push_back(data);
        temp_ue_data.erase(rnti);
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
            store_data(rnti);
        }
        else {;}
    }


    void exportToCSV(const std::string& filename) {
        std::ofstream csvFile(filename);

        // Write CSV header
        csvFile << "timestamp,rnti,ue_id,state,ph,pcmax,rsrp,cqi,dl_ri,ul_ri,";
        csvFile << "dlsch_err,pucch_dtx,dl_bler,dl_mcs,ulsch_err,ulsch_dtx,";
        csvFile << "ul_bler,ul_mcs,nprb,snr" << std::endl;

        for (const auto& [rnti, records] : ue_data_by_rnti) {
            for (const auto& data : records) {
                char timeBuffer[30];
                strftime(timeBuffer, sizeof(timeBuffer), "%Y-%m-%d %H:%M:%S", localtime(&data.timestamp));
                // Write data row for this RNTI
                csvFile << timeBuffer << ","
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
                << data.ulsch_err << ","
                << data.ulsch_dtx << ","
                << data.dl_bler << ","
                << data.dl_mcs << ","
                << data.ul_bler << ","
                << data.ul_mcs << ","
                << data.nprb << ","
                << data.snr << std::endl;
            }
        }
        csvFile.close();
    }
};


int main(int argc, char* argv[]) {
    Parser parser;

    std::string line;
    std::string outputFile = "ue_metrics.csv";

    if (argc > 1) {
        outputFile = argv[1];
    }
    freopen("../gnb.log", "r", stdin); // FOR TESTING
    while(std::getline(std::cin, line)) {
        if (!line.empty()) {
            try {
                parser.parse_line(line);
            } catch (const std::exception &e) {
                std::cerr << "Error parsing line: " << line << std::endl;
                std::cerr << "Exception: " << e.what() << std::endl;
            }
        }
    }
    parser.exportToCSV(outputFile);
}