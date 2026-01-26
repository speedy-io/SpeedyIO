#include <stdio.h>
#include <time.h>
#include <ctime>
#include <unistd.h> // for sleep()
//#include "crypto_utils.h"
#include "LicenseValidation.h"



// Function to print a time_t value in a readable format
void print_readable_time(time_t time_val) {
    struct tm *timeinfo;
    timeinfo = localtime(&time_val);
    
    char buffer[80];
    strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", timeinfo);
    
    printf("Time is: %s\n", buffer);
}

// Check if the program has been running for more than 'max_run_time' seconds
int check_license_expired_time_elapsed(time_t start_time) {
    time_t now;
    time(&now);

    int max_run_time = 9999;

    if (difftime(now, start_time) > max_run_time) {
        return 1; // Time limit exceeded
    }
    return 0; // Still within the allowed time
}

// Print a progress bar
void print_progress_bar(int step, int total_steps) {
    int bar_width = 20;
    int pos = (step * bar_width) / total_steps;

    printf("\r[");
    for (int i = 0; i < bar_width; ++i) {
        if (i < pos) printf("=");
        else if (i == pos) printf(">");
        else printf(" ");
    }
    printf("] %d%%", (step * 100) / total_steps);
    fflush(stdout); // Ensure the progress bar is updated immediately on the terminal
}

int main() {
    time_t start_time;
    time(&start_time);
    printf("Program started...\n");
    print_readable_time(start_time);
    printf("\n");

    while (1) {
	
            //THis
	std::map<std::string, std::string> licenseInfo = validateAndLoadLicense("key.txt", "iv.txt", "public.pem", "license.lic", "signature.txt");
	std::cout << "\nDecrypted License Info:" << std::endl;
	for (const auto& pair : licenseInfo) {
	    std::cout << pair.first << ": " << pair.second << std::endl;
	}
	std::cout << std::endl;

	time_t expiry = string_to_time_t(licenseInfo["endDate"]);

        if (
		check_license_expired_target_date(expiry) //true false
		// check_license_expired_time_elapsed(start_time)
	) {
            printf("\nYour license has expired. Please renew your license.\n");
            break; // Exit the loop if any license condition fails
        }

        // Update progress every second over 10 seconds
        for (int i = 0; i < 10; ++i) {
            print_progress_bar(i + 1, 10);
            sleep(1);
        }

        // Reset progress for next loop
        printf("\nProgram is running. Your license is active.\n");
    }

    // Program ends when the license expires
    return 0;
}
