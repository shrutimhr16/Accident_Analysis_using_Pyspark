# Accident Analysis Case Study
> The data shared for the first round is a sample database of vehicle accidents across US for brief amount of time. Use this data to perform the following analysis:

## Business objective
a.	Application should perform below analysis and store the results for each analysis.
1.	Analytics 1: Find the number of crashes (accidents) in which number of males killed are greater than 2?
2.	Analysis 2: How many two wheelers are booked for crashes? 
3.	Analysis 3: Determine the Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy.
4.	Analysis 4: Determine number of Vehicles with driver having valid licences involved in hit and run? 
5.	Analysis 5: Which state has highest number of accidents in which females are not involved? 
6.	Analysis 6: Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death
7.	Analysis 7: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style  
8.	Analysis 8: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
9.	Analysis 9: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
10.	Analysis 10: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)

<br>
## Steps :
1. clone repository
2. Run Spark Submit
spark-submit main.py

## Conclusion
1. Find the number of crashes (accidents) in which number of males killed are greater than 2. -  3000
2. How many two wheelers are booked for crashes? 784
3. Determine the Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy. 
-['CHEVROLET', 'FORD', 'DODGE', 'FREIGHTLINER', 'NISSAN']
4. Determine number of Vehicles with driver having valid licences involved in hit and run?-  6359
5. Which state has highest number of accidents in which females are not involved? - Texas
6. Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death: ['TOYOTA', 'DODGE', 'NISSAN']
7.For all the body styles involved in crashes, mention the top ethnic user group of each unique body style.
+---------------------------------+-----------------+
|VEH_BODY_STYL_ID                 |PRSN_ETHNICITY_ID|
+---------------------------------+-----------------+
|AMBULANCE                        |WHITE            |
|BUS                              |HISPANIC         |
|FARM EQUIPMENT                   |WHITE            |
|FIRE TRUCK                       |WHITE            |
|MOTORCYCLE                       |WHITE            |
|NEV-NEIGHBORHOOD ELECTRIC VEHICLE|WHITE            |
|PASSENGER CAR, 2-DOOR            |WHITE            |
|PASSENGER CAR, 4-DOOR            |WHITE            |
|PICKUP                           |WHITE            |
|POLICE CAR/TRUCK                 |WHITE            |
|POLICE MOTORCYCLE                |HISPANIC         |
|SPORT UTILITY VEHICLE            |WHITE            |
|TRUCK                            |WHITE            |
|TRUCK TRACTOR                    |WHITE            |
|VAN                              |WHITE            |
|YELLOW SCHOOL BUS                |WHITE            |
+---------------------------------+-----------------+

8. Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash  ['76010', '78521', '75067', '78574', '75052']
9. Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance [14870169, 14894076, 14996273, 15232090, 15232090, 15249931, 15307513]
10. Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences: ['FORD', 'CHEVROLET', 'TOYOTA', 'DODGE', 'NISSAN']


<br>

## Technologies Used
- Python - version 3.x , Spark version 3.4.1

## Contributors
* Shruti Mehra
