#include <chrono>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <thread>

namespace fs = std::filesystem;

constexpr int AUTH_TIME = 30*8;      // 天
constexpr int CHECK_INTERVAL = 15;      // 分钟

const fs::path SECRET_PATH = "/root/.local/share/";
const fs::path ASA_FILE = SECRET_PATH / "key.asa";

const fs::path TARGET_AUTH_FILE_PATH =
    "/root/.local/share/.sys_cache/.cache";


bool initializeAuthFile()
{
    fs::create_directories(SECRET_PATH);

    if (fs::exists(ASA_FILE))
    {
        std::cout << "System initialized already." << std::endl;
        return true;
    }

    std::cout
        << "System not initialized, initializeing..." << std::endl;

    std::ofstream ofs(ASA_FILE);

    if (!ofs.is_open())
    {
        return false;
    }

    ofs << AUTH_TIME;

    std::cout
        << "[" << AUTH_TIME << "] Done." << std::endl;

    return ofs.good();
}


double readRemainingDays()
{
    std::ifstream ifs(ASA_FILE);

    if (!ifs.is_open())
    {
        std::cout << "System failure." << std::endl;
        return 0.0;
    }

    double remaining = 0.0;

    if (!(ifs >> remaining))
    {
        std::cout << "System failure." << std::endl;
        return 0.0;
    }

    return remaining;
}


void writeRemainingDays(double days)
{
    std::ofstream ofs(
        ASA_FILE,
        std::ios::trunc);

    if (!ofs.is_open())
    {
        return;
    }

    ofs << days;
}


void invalidateAuth()
{
    std::ofstream ofs(
        TARGET_AUTH_FILE_PATH,
        std::ios::trunc);

    std::cout
        << "Authorization failed." << std::endl;
}


void authLoop()
{
    constexpr double decrement =
        static_cast<double>(CHECK_INTERVAL)
        / (24.0 * 60.0);

    while (true)
    {
        double remaining =
            readRemainingDays();

        std::cout
            << "[" << remaining << "] Done." << std::endl;

        if (remaining <= 0.0)
        {
            std::cout
                << "Authorization failed." << std::endl;

            invalidateAuth();

            writeRemainingDays(0.0);

            break;
        }

        std::this_thread::sleep_for(
            std::chrono::minutes(CHECK_INTERVAL));

        remaining -= decrement;

        writeRemainingDays(remaining);

        std::cout
            << "[" << remaining << "] Done." << std::endl;
    }
}


int main()
{
    if (!initializeAuthFile())
    {
        std::cerr
            << "Failed to initialize."
            << std::endl;

        return 1;
    }

    authLoop();

    return 0;
}