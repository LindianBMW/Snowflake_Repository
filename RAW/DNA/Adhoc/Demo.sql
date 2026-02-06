

/********************************************************************* */
-- ================         Demo  Start ============================== --

/********************************************************************* */

--1. Truncate the destination table
 truncate table  RAW.CRM.INFINITY_DOWNLOAD;

--2. select from destination table to confirm truncate
select *   from RAW.CRM.INFINITY_DOWNLOAD;

--3. Check that the stage is empty
remove @RAW.CRM.INFINITY;
list  @RAW.CRM.INFINITY;

 --3. Simulate Server side processing
/*/
    a. At a certain time in the day a connection wil be made to
       the Infinity data source.
    b. Then data will be extracted and placed into a staging area
    All the above processing will be handled by this script which will be started on the server. 
    We still have no access to a server, but it it done here for demo purposes.
    In effect all we do on the server is call this script 
    downloadInfinitydata.ps1  at a certain during the day

    The Infinity data is stored as a .csv  in a local folder
    mention 13 month back initial load , thereafet incremental 
    
    Then we use snowsql to PUT the data into the stage

    C:\Data\Infinity\Scripts 
*/ 

--4. Now look at stage to confirm data is there ..(Run it on the server) 
 list  @RAW.CRM.INFINITY;

 --5. Execute the schedule  - time will be set, but for demo, we will just run it

 execute task RAW.CRM.task_load_infinity_download; 

 select *   from RAW.CRM.INFINITY_DOWNLOAD

/********************************************************************* */
-- ================       Demo End ============================== --

/********************************************************************* */

