
I had to build a webhook event processor using lambda.
The lambda would receive the event, query db using values from the event, and for every value returned by the db, perform some updates back into the db.

While building this, I had to keep in mind: Lambdas in AWS have time limit and therefore we ha to put a limit on how many results from the db can be procesed by one single lambda

In order for it to be scalable, I followed this architecture:
(a) Create one lambda for receving the webhook event and send output to the queue
(b) Create second lambda which would receive event from above queue, and 
    (i) put it back to the queue if result-size was greater than the limit (of results from db) that lambda can process
    (ii) If not, send it to another queue
(c) This lambda would take messages from the second queue and perform the updates one by one
