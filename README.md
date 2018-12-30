# Big Data Projects
This repository stores codes of two projects in my Big Data Analytics and Management course.

## Project1 
This project was written in Java using **Hadoop**. There are total 4 questions of this project.

### Question1
Requirement: Write a MapReduce program in Hadoop that implements a simple “Mutual/Common friend list of two friends. I was given 5 pairs of friends and asked to output their mutual friends list.
I used two Hadoop jobs to solve this question.

  - job1:
    - **map phase**: find friend pair(**note: small user_id should be before large user_id, in this way, during the reduce phase, Hadoop can find common keys**).
    - **reduce phase**: find common friend list.
  - job2:
    - **map phase**: for job2, I only used the map phase in order to output the specific 5 pairs' common friends list.

### Question2
Requirement: Using the intermediate output of question1, find friend pairs whose number of common friends (number of mutual friend) is within the top-10 in all the pairs. Output them in decreasing order.
I used one Hadoop job to solve this question.

  - job1:
    - **map phase**: read in common friends list and get the length of the list. Then, use the length as key, friend pair as the value. Write the key-value pairs into reducer.
    - **reduce phase**: since we set `job.setSortComparatorClass(LongWritable.DecreasingComparator.class);`, reducer will receive the decreasing order result automatically. Hence, during the reduce phase, I only need to output the first 10 pieces of data.

### Question3
Requirement: Given any two Users (they are friend) as input, Use in-memory join to output the list of the names and the states of their mutual friends.
I used one Hadoop job to solve this question.

  - job1:
    - **map phase**: for this question, I only use the mapper and didn't use the reducer, since I was asked to implement **in-memory join**. First, I read the user info into the memory during the **setup** phase of the mapper. Then, I used the user info in the memory to directly match two users' common friends' name and states info.

### Question4
Requirement: Using reduce-side join and job chaining:

  - Step 1: Calculate the minimum age of the direct friends of each user.
  - Step 2: Sort the users by the calculated minimum age from step 1 in descending order.
  - Step 3. Output the top 10 users from step 2 with their address and the calculated minimum age.

I used 3 Hadoop jobs to solve this question.

  - job1: I used two mappers and one reducer for this job. Also, I create a `KeyPair` object to **enable sorting of secondary key**.
    - **UserDataAgeMap**: in this mapper, I calculated the age of each user. And the output format is **((user_id, A:age), null)**. The reason why I put an **A** before age is to make sure that age will always come before **friends relation** during the reduce phase.
    - **ListFriendsMap**: in this mapper, I output the friend pair, and the format is **((user2_id, B:user1_id), null)**.
    - **reduce phase**: I used a Map to store users and their minimum age friend. At the `cleanup` phase, the final minimum age and user_id were passed to next job.
  - job2:
    - **map phase**: I used the age as key and user_id as value. Then, passed the pair into reduce phase.
    - **reduce phase**:since I set `job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);`, during the reduce phase, I directly output the first 10 pieces of data.
  - job3: I used two mappers and one reducer for this job. 
    - **Top10UserMap**: retrieve the result from job2.
    - **AddressMap**: get the address info from user info file.
    - **reduce phase**: match the user with minimum age of friend and their address.

## Project2
This project was written in Python using **Spark**. There are total 4 questions of this project. And for each question, I was asked to solve the question using **spark rdd** and **spark sql** separately.
### Question1
Requirement: Write a spark script to find total number of common friends for any possible friend pairs. This question is the same as in Project1-Question1, however, it required me to use Spark to solve the question. The ideas to solve this question was the same as Project1-Question1. For detailed implementation, please see the actual Python codes.

### Question2
Requirement: Find top-10 friend pairs by their total number of common friends. For each top-10 friend pair print detail information in decreasing order of total number of common friends. The ideas to solve this question was the same as Project1-Question2. For detailed implementation, please see the actual Python codes.

### Question3
Requirement: List the **user_id** and **rating** of users that reviewed businesses located in **Stanford**. For detailed implementation, please see the actual Python codes.

### Question4
Requirement: List the **business_id**, **full address** and **categories** of the Top 10 businesses using the average ratings. For detailed implementation, please see the actual Python codes. 


              
        
    




