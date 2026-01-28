 # Variables
$apiKey    = "e43a866d-5c41-4fee-8d87-6fce68b57cd2"
$igrpId    = "2175"
# Calculate start date: 1st day of the month, 13 months before today
$today = Get-Date
$startDate = (Get-Date -Year $today.AddMonths(-13).Year -Month $today.AddMonths(-13).Month -Day 1 -Hour 0 -Minute 0 -Second 0).ToString('yyyy-MM-dd')
$endDate   = $today.ToString('yyyy-MM-dd')
$limit     = 99999   # maxinum to 1000

# Ensure output directory exists
$outputDir = "C:\Data\Infinity\Output"
if (-not (Test-Path $outputDir)) {
    New-Item -Path $outputDir -ItemType Directory | Out-Null
}

# Format date ranges for filename (no special chars)
$startStr = $startDate -replace '[^0-9]', '-'
$endStr = $endDate -replace '[^0-9]', '-'
$outputPath = "$outputDir\InfinityCalls_${startStr}_to_${endStr}.csv"
 
# List ONLY API keys you see in actual data
$fields = @(
  "rowId",
  "triggerDatetime",
  "igrp",
  "dgrp",
  "ch",
  "src",
  "act",
  "algo",
  "attr",
  "vref",
  "href",
  "num",
  "term",
  "vid",
  "t",
  "goal",
  "srcHash",
  "new",
  "pageTitle",
  "pub",
  "segment",
  "segmentGroupId",
  "dom",
  "ref",
  "network",
  "matchRef",
  "matchType",
  "campaign",
  "adGroup",
  "adRef",
  "keywordRef",
  "dialledPhoneNumber",
  "srcPhoneNumber",
  "destPhoneNumber",
  "callDuration",
  "bridgeDuration",
  "ringTime",
  "ivrDuration",
  "queueDuration",
  "operatorRef",
  "ivrRef",
  "dialplanRef",
  "callRating",
  "callState",
  "callDirection",
  "callStage",
  "operatorRealm",
  "telcoCode",
  "numType",
  "rec",
  "transcriptionConfidence",
  "totalKeywordScore",
  "totalOperatorKeywordScore",
  "totalContactKeywordScore",
  "operatorPositiveKeywordCount",
  "operatorNeutralKeywordCount",
  "operatorNegativeKeywordCount",
  "contactPositiveKeywordCount",
  "contactNeutralKeywordCount",
  "contactNegativeKeywordCount",
  "callPciDataChecked",
  "callPciDataFound",
  "callSsnDataChecked",
  "callSsnDataFound",
  "callPiiDataChecked",
  "callPiiDataFound",
  "callKeywordSpotting",
  "callTranscription",
  "whois",
  "ip",
  "ua",
  "country",
  "city",
  "continent",
  "res",
  "lat",
  "long",
  "region",
  "postcode",
  "area",
  "spider",
  "host",
  "visitorType",
  "sfWhoRef",
  "visitorPageCount",
  "visitorGoalCount",
  "visitorCallCount",
  "visitorFirstDatetime",
  "landingPageId",
  "conversionPageId",
  "chName",
  "chType",
  "segmentName",
  "segmentRef",
  "orgId",
  "segmentGroupName",
  "notes",
  "leadScore"
)

# Build display[] part for the API URL
$displayParams = ($fields | ForEach-Object { "display[]=$_"} ) -join "&"

# Build full URL
$url = "https://api.infinitycloud.com/reports/v2.1/igrps/$igrpId/triggers/calls?startDate=$startDate&endDate=$endDate&limit=$limit&format=csv&$displayParams"

# Headers
$headers = @{
    "Authorization" = "ApiKey $apiKey"
}

# Download and save to CSV
Invoke-RestMethod -Uri $url -Headers $headers -Method Get -OutFile $outputPath

# Log the run to a history file
Add-Content -Path "$outputDir\run_history.log" -Value "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') - Ran script, output: $outputPath"

# Remove all files from the Snowflake stage before uploading
$removeCmd = '"C:\Program Files\Snowflake SnowSQL\snowsql.exe" -c bigmotoringworld_snowflake -q "REMOVE @RAW.CRM.INFINITY;" > C:\Data\Infinity\Output\snowsql_remove.log 2>&1'
cmd.exe /c $removeCmd

# Upload all Infinity*.csv files to Snowflake stage and log output
$putCmd = '"C:\Program Files\Snowflake SnowSQL\snowsql.exe" -c bigmotoringworld_snowflake -q "PUT file://C:/Data/Infinity/Output/Infinity*.csv @RAW.CRM.INFINITY auto_compress=false;" > C:\Data\Infinity\Output\snowsql_upload.log 2>&1'
cmd.exe /c $putCmd

Write-Host "Requested Infinity report with correct API columns saved to $outputPath"