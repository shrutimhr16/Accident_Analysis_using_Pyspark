from pyspark.sql import SparkSession

from src.utils import read_yaml , get_spark
from src.analysis import AccidentAnalysis

if __name__ == "__main__":
    # Initialize spark session
    spark=get_spark("AccidentAnalysis")
    config_file_name = "config.yaml"
    spark.sparkContext.setLogLevel("ERROR")

    config = read_yaml(config_file_name)
    output_file_paths = config.get("OUTPUT_PATH")
    file_format = config.get("FILE_FORMAT")

    accident_analysis = AccidentAnalysis(spark, config)

    # 1. Find the number of crashes (accidents) in which number of males killed are greater than 2?
    print(
        "1. Find the number of crashes (accidents) in which number of males killed are greater than 2.",
        accident_analysis.count_male_accidents(output_file_paths.get(1), file_format.get("Format")),
    )

    # 2. How many two-wheelers are booked for crashes?
    print(
        "2. How many two wheelers are booked for crashes?",
        accident_analysis.count_two_wheeler_accidents(
            output_file_paths.get(2), file_format.get("Format")
        ),
    )

    # 3. Determine the Top 5 Vehicle Makes of the cars present in the crashes in which 
    # driver died and Airbags did not deploy..
    print(
        "3. Determine the Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy.",
        accident_analysis.display_top_5_car_brands_with_faulty_airbags_accidents(
            output_file_paths.get(3), file_format.get("Format")
        ),
    )

    # 4. Determine the number of Vehicles with a driver having valid licences involved in hit-and-run?
    print(
        "4. Determine number of Vehicles with driver having valid licences involved in hit and run?",
        accident_analysis.count_drivers_with_hitandrun_and_valid_license(output_file_paths.get(4), file_format.get("Format"))
        ),
    

    # 5. Which state has the highest number of accidents in which females are not involved?
    print(
        "5. Which state has highest number of accidents in which females are not involved?",
        accident_analysis.state_with_max_accidents_without_females(
            output_file_paths.get(5), file_format.get("Format")
        ),
    )

    # 6. Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death
    print(
        "6. Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death:",
        accident_analysis.display_top_3_to_5_veh_make_id_involved_in_injuries(
            output_file_paths.get(6), file_format.get("Format")
        ),
    )

    # 7. For all the body styles involved in crashes, mention the top ethnic user group of each unique body style
    print("7.For all the body styles involved in crashes, mention the top ethnic user group of each unique body style.")
    accident_analysis.display_top_ethnic_group_with_body_style(
        output_file_paths.get(7), file_format.get("Format")
    ).show(truncate=False)

    # 8. Among the crashed cars, what are the Top 5 Zip Codes with the highest number of crashes with alcohol as the
    # contributing factor to a crash (Use Driver Zip Code)
    print(
        "8. Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash ",
        accident_analysis.get_top_5_zip_codes_had_crashes_due_to_alcohol(
            output_file_paths.get(8), file_format.get("Format")
        ),
    )

    # 9. Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above
    # 4 and car avails Insurance
    print(
        "9. Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance",
        accident_analysis.display_crash_ids_with_no_damage(
            output_file_paths.get(9), file_format.get("Format")
        ),
    )

    # 10. Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed
    # Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of
    # offenses (to be deduced from the data)
    print(
        "10. Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences:",
        accident_analysis.display_top_5_vehicle_brand(
            output_file_paths.get(10), file_format.get("Format")
        ),
    )

    spark.stop()
