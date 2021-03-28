                             ************************
                             * Project Introduction *
                             ************************

Throughout the last week, while also holding down a full time job, I derived the solutions
to all the tasks posed in a DataBricks 'coding challenge' - except for a workable solution for the Logistical
regression question. I will explain my assumptions and reasoning for my current solutions
for each of the tasks I attempted, in this file. I want to stress that in a normal workflow, I would
have assigned a lot more time to testing the solutions derived. Also, in a real life scenario,
I expect that I would need to segregate the solutions so that I can generate independently runnable
spark binary files.

                                 *****************
                                 ** Assumptions **
                                 *****************
Groceries Tasks
- The assumption I made here was that the file was actually meant to be downloaded as part
  of the program hence, I have the file being read via the URL on start up.
- The grocery transactions did not contain any duplicate data and even if they did, it would
  be indistinguishable because there were no unique identifiers attached to the transactions.
- When I had to derive a list of all the unique products present in the transactions, I concluded
  that because the question emphasized 'transactions' (the plural of transaction), that the
  requirement was to find out all the unique products within the entire transactions list - rather
  than the unique products listed per transaction.

AirBnB Tasks
- When i tried to download the file from the github location i noticed that the GitHub location was
  not from a 'usercontent' location like the first file so, I decided to manually download this file
  to my local machine and add it to this project directly.
- My biggest assumption made in this set of tasks was regarding task 4. I concluded that the
  filtering criteria for the dataset was driven by finding the property with 'the lowest price
  and highest rating'. Then once the properties were in ranking order and weighted,
  to decide on the exact property which this criteria, then I needed to find how many people this
  property could actually accommodate.

AirFlow Task
-  Initially it wasn't clear to me how much of this I had to actually do however, I concluded that
   I couldn't be fully sure that code i would write in a .py file concerning the required DAG would work -
   unless I set up Airflow on my local machine. So, I set up a AirFlow webserver and drove the
   development of this DAG based on what I could visually see getting represented on the Airflow UI. One
   conundrum I faced with teh DAG was - how to represent the 2 parallel running tasks
   so that they would transition over to the next 3 tasks (which needed to be ran in parallel also)
   without triggering them more than one time? I concluded that I needed to add another node to join the
   two nodes (which ran in parallel) and then call the three other tasks ONE time.
-  To run the DAGs in parallel you need to ensure that the airflow.cfg has LocalExecutor set instead
   of SequentialExecutor, as the 'executor' property's value.

Logistical Regression Task
-  I initially thought that I was going to hit the ground running with this task however, once
   I realized that it was a multinomial logistical regression solution that i needed to solve
   this problem it became clear that I would have to tackle this differently to my initially
   assumed binary logistical regression solution. Based on the spark online documentation and
   the developer notes linked to the ML library, i didn't find them very intuitive for piecing together
   how i needed to leverage the Spark ML library to solve the problem. For that reason, I decided
   to strip things down to there basics and understand the formula's used from a maths stand point.
   Hence, I pulled out the two following online pages:

   https://bookdown.org/chua/ber642_advanced_regression/multinomial-logistic-regression.html
   https://www.theanalysisfactor.com/interpreting-regression-coefficients/

   In the end I decided that my coding challenge should be 'timed out' on my end because, I have
   other obligations that I need to pick up. However, I would like to complete this ML spark task
   at some point as I can visualize want i need to do - I just need to spend more time reviewing the
   maths/Spark ML library so that I can combine the two into a workable/resilient solution.