param
(
    [string]$dbServer = "",
    [string]$dbName = "",
    [string]$dbUser = "",
    [string]$dbPassword = "",
    [string]$awsProfile = "",
    [string]$storageAccountName = "",
    [string]$storageAccountKey = "",
    [string]$createBucket = "",
    [string]$bucketName = "",
    [string]$tables = "",
    [string]$isMinio = "",
    [string]$isAzure = ""
)

function Move-DataToAWS([string]$tableName, [string]$primaryKeyColumnName, [string]$contentColumnName, [bool]$isIdColumnInt = $true) {
$hasResults = 1;
while ($hasResults -eq 1) {
    $totalItemsToImport = $msmDatabase.ExecuteWithResults("SELECT COUNT(*) as totalItems FROM $tableName WHERE externalStorageProvider IS NULL;").Tables[0].totalItems
    $totalItems = 1000;
    if ($totalItems -gt 0) { 
        echo "Moving $tableName data to external storage..."
    } else {
        echo "Found no data to move in $tableName table!"
        return
    }
    
    $progress = 0;
    $failedCount = 0;
    $successCount = 0;

    $results = $msmDatabase.ExecuteWithResults("SELECT TOP (1000) $primaryKeyColumnName AS id, $contentColumnName AS content FROM $tableName WHERE externalStorageProvider IS NULL;")
    if ($results) {

    $results.Tables[0] | ForEach-Object {
        $id = $_.id
        $content = $_.content
        $guid = [guid]::NewGuid()
        if ($isAzure -eq 1) {
            $storageContext = New-AzStorageContext -StorageAccountName $storageAccountName -StorageAccountKey $storageAccountKey
            $externalStorageProvider = "{`"Type`":`"AzureBlob`",`"ExternalStorageKey`":`"$guid`",`"BucketName`":`"$bucketName`",`"ServiceUrl`":`"https://$storageAccountName.blob.core.windows.net/$bucketName`"}"
        } else {
           $externalStorageProvider = "{`"Type`":`"S3`",`"ExternalStorageKey`":`"$guid`",`"BucketName`":`"$bucketName`",`"Region`":{`"SystemName`":`"$($region.Region)`",`"DisplayName`":`"$($region.Name)`"}}"
        }
        try {       
            if ($isAzure -eq 1) {
                $tempFilePath = [System.IO.Path]::GetTempFileName()
                if ($content.GetType().Name -eq "byte[]") {  
                   [System.IO.File]::WriteAllBytes($tempFilePath, $content)
                    Set-AzStorageBlobContent -Container $bucketName -Blob $guid -Context $storageContext -File $tempFilePath
                    Remove-Item $tempFilePath
                 } elseif ($content.GetType().Name -eq "string") {
                    $content | Out-File -FilePath $tempFilePath -Encoding UTF8
                    Set-AzStorageBlobContent -Container $bucketName -Blob $guid -Context $storageContext -File $tempFilePath
                    Remove-Item $tempFilePath
            }
           
            } else {
                # AWS or Minio upload
                if ($content.GetType().Name -eq "byte[]") { 
                   $memoryStream = New-Object -TypeName 'System.IO.MemoryStream' -ArgumentList (,$content)
                   if ($isMinio -eq 1) {
                      Write-S3Object -EndpointUrl http://localhost:9000 -BucketName $bucketName -Stream $memoryStream -Key $guid -CannedACLName bucket-owner-full-control
                   } elseif ($isAzure -ne 1) {
                      Write-S3Object -BucketName $bucketName -Stream $memoryStream -Key $guid -CannedACLName bucket-owner-full-control
                   }
                } elseif ($content.GetType().Name -eq "string") {
                   if ($isMinio -eq 1) {
                      Write-S3Object -EndpointUrl http://localhost:9000 -BucketName $bucketName -Content $content -Key $guid -CannedACLName bucket-owner-full-control
                    } elseif ($isAzure -ne 1) {
                      Write-S3Object -BucketName $bucketName -Content $content -Key $guid -CannedACLName bucket-owner-full-control
                   }
                }
            }

            # update msm databse to null content and specify externalStorageProvider			
			if($isIdColumnInt) {
				$msmDatabase.ExecuteNonQuery("UPDATE $tableName SET $contentColumnName = NULL, externalStorageProvider = '$externalStorageProvider' WHERE $primaryKeyColumnName = $id")
			} else {
				$msmDatabase.ExecuteNonQuery("UPDATE $tableName SET $contentColumnName = NULL, externalStorageProvider = '$externalStorageProvider' WHERE $primaryKeyColumnName = '$id'")
			}
			
			$successCount++
        } catch {
            $failedCount++;
            echo "An exception occured while proccessing $primaryKeyColumnName - {$id}: $_"
        } finally {
            $progress++;
            $percentageComplete = ($progress / $totalItems) * 100
            Write-Progress -Activity "Move in Progress" -Status "$percentageComplete% Complete:" -PercentComplete $percentageComplete;
        }
    }
    echo "Total items are $totalItemsToImport";
    if ($totalItemsToImport -gt 0) {
        echo "Move completed. Successfully moved $successCount/$totalItems rows to storage, now have $totalItemsToImport rows left to transfer..."
    } else {
     $hasResults = 0;
    }
    }
}
}

# ensure we're elevated
$isAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole] "Administrator")
if (!$isAdmin) {
    try {
        # we're not running elevated - so try to relaunch as administrator
        echo "Starting elevated PowerShell instance."
        $newProcess = New-Object System.Diagnostics.ProcessStartInfo "PowerShell"
        $newProcess.Arguments = @("-NoProfile","-NoLogo", $myInvocation.MyCommand.Definition, "-storageAccountKey `"$storageAccountKey`"  -storageAccountName `"$storageAccountName`" -isAzure `"$isAzure`"  -dbServer `"$dbServer`" -dbName `"$dbName`" -dbUser `"$dbUser`" -dbPassword `"$dbPassword`" -awsProfile `"$awsProfile`" -createBucket `"$createBucket`" -bucketName `"$bucketName`" -tables `"$tables`"")
        $newProcess.Verb = "runas"
        [System.Diagnostics.Process]::Start($newProcess)
    }
    catch {
        echo "Unable to start elevated PowerShell instance."
    }
    finally {
        # always exit this script either we're now running a separate elevated power shell or we've had an error
        exit
    }
}

[System.Net.ServicePointManager]::SecurityProtocol = [System.Net.SecurityProtocolType]"Ssl3,Tls,Tls11,Tls12"

# ensure we have the Microsoft.SqlServer.Smo module installed
$sqlServerModules = Get-Module -ListAvailable -Name SqlServer

if ($sqlServerModules) {
    Import-Module SqlServer
}

[Reflection.Assembly]::LoadWithPartialName("Microsoft.SqlServer.Smo") | Out-Null

if (!$sqlServerModules) {
    try {
        New-Object Microsoft.SqlServer.Management.SMO.Database | Out-Null
    } catch {
        echo "Installing SqlServer module..."
        Install-Module SqlServer -Scope CurrentUser
        Import-Module SqlServer
        [Reflection.Assembly]::LoadWithPartialName("Microsoft.SqlServer.Smo") | Out-Null
    }
}

if ($isAzure -eq 1) {
    $AzurePowerShellModules = Get-Module -ListAvailable -Name Az
    if ($AzurePowerShellModules) {
        Import-Module Az
    }
    if (!$AzurePowerShellModules) {
            echo "Installing AZ Powershell module..."
            Install-Module -Name Az -Repository PSGallery -Force
            Import-Module Az
    }
}

# ensure we have the AWS CLI installed
if ($isAzure -ne 1) {
$cmdOutput = aws --version

if (!$cmdOutput.StartsWith("aws-cli")) {
    echo "AWS CLI not found! Please install AWS CLI and retry the operation. https://awscli.amazonaws.com/AWSCLIV2.msi"
    exit
}

# ensure we have the AWSPowerShell module installed

$awsPowerShellModules = Get-Module -ListAvailable -Name AWSPowerShell

if ($awsPowerShellModules) {
    Import-Module AWSPowerShell
}

if (!$awsPowerShellModules) {
    try {
        Set-AWSCredential
    } catch {
        echo "Installing AWSPowerShell module..."
        Install-Module AWSPowerShell -Scope CurrentUser
        Import-Module AWSPowerShell
    }
}

# check if we must use the default profile or the user specified profile as well as if it has a region set
$awsProfile = if (!$awsProfile) {"default"} else {$awsProfile}

if ((Get-AWSCredential -ListProfileDetail | Where-Object {$_.ProfileName -eq $awsProfile}).count -eq 1) {
    Set-AWSCredential -ProfileName $awsProfile # set default region for this session using the profiles region
    Set-DefaultAWSRegion (aws configure get region --profile $awsProfile)
    $region = (Get-DefaultAWSRegion) # set global region for access by functions 

    # ensure a default region has been set using the region found on the profile specified
    if ((Get-DefaultAWSRegion).count -eq 0) {
        echo "Please set a default region on the AWS profile you provided and try again"
        exit
    }

    echo "Using AWS profile: $awsProfile"
    echo "Using AWS region: $($region.Region)"
} else {
    echo "No profile found for $awsProfile, please specify -awsProfile with a valid profile or create the missing profile"
    exit
}
}


# check if we must create or use an existing bucket and whether or not the existing bucket exists and is valid

   
   if ($isMinio -eq 1) {

    if ((Get-S3Bucket -EndpointUrl http://localhost:9000 | Where-Object {$_.BucketName -eq $bucketName}).count -eq 0) {
        echo "Bucket $bucketName not found. Please specify a valid existing bucket name or a new one with -createBucket"
        exit
    }
    }
    elseif ($isAzure -ne 1) {
  
       if ((Get-S3Bucket | Where-Object {$_.BucketName -eq $bucketName}).count -eq 0) {
        echo "Bucket $bucketName not found. Please specify a valid existing bucket name or a new one with -createBucket"
        exit
    }
    
    }

# ensure db credentials are specified
if (!$dbServer -or !$dbName -or !$dbUser) {
    echo "-dbServer, -dbName and -dbUser MUST be specified!"
    exit
}

# ensure tables are specified
if(!$tables) {
    echo "Please specify tables to move data to S3 API using -tables table1,table2 table1 and table2 are simply examples."
    exit
}

$server = New-Object Microsoft.SqlServer.Management.SMO.Server(New-Object Microsoft.SqlServer.Management.Common.ServerConnection($dbServer, $dbUser, $dbPassword))
$msmDatabase = New-Object Microsoft.SqlServer.Management.SMO.Database($server, $dbName)

$tableArray = $tables.Split(",")
Foreach ($table in $tableArray) {
    switch($table) {
        "queuedNotification" { Move-DataToAWS -tableName queuedNotification -primaryKeyColumnName queuedNotificationId -contentColumnName content }
        "attachment" { Move-DataToAWS -tableName attachment -primaryKeyColumnName attachmentId -contentColumnName content }
		"note" { Move-DataToAWS -tableName note -primaryKeyColumnName noteIdentifier -contentColumnName content }
		"richTextImage" { Move-DataToAWS -tableName richTextImage -primaryKeyColumnName fileName -contentColumnName content -isIdColumnInt $false }
    }
}
