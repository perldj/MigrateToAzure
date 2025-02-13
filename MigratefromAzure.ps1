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

function Move-DataFromExternalStorage([string]$tableName, [string]$primaryKeyColumnName, [string]$contentColumnName, [bool]$isIdColumnInt = $true) {

    $hasResults = 1;
    while ($hasResults -eq 1) {

        $totalItemsToRestore = $msmDatabase.ExecuteWithResults(
            "SELECT COUNT(*) as totalItems 
             FROM $tableName
             WHERE externalStorageProvider IS NOT NULL
               AND $contentColumnName IS NULL;"
        ).Tables[0].totalItems

        if ($totalItemsToRestore -eq 0) {
            Write-Host "Found no data to restore in $tableName table!"
            return
        }
        else {
            Write-Host "Restoring $tableName data from external storage..."
        }

        $results = $msmDatabase.ExecuteWithResults(
            "SELECT TOP (1000)
                    $primaryKeyColumnName AS id,
                    externalStorageProvider
             FROM $tableName
             WHERE externalStorageProvider IS NOT NULL
               AND $contentColumnName IS NULL;"
        )

        if (!$results) {
          
            return
        }

        $rows = $results.Tables[0]
        $progress = 0
        $failedCount = 0
        $successCount = 0

        foreach ($row in $rows) {

            $id = $row.id
            
            $extJson = $row.externalStorageProvider
            if ([string]::IsNullOrEmpty($extJson)) {
                continue
            }

            try {
               
                $extObj = $extJson | ConvertFrom-Json

            
                $blobKey   = $extObj.ExternalStorageKey
                $bucket    = $extObj.BucketName

            
                $restoredContent = $null

                if ($isAzure -eq 1) {
                  
                    $storageContext = New-AzStorageContext -StorageAccountName $storageAccountName -StorageAccountKey $storageAccountKey

                   
                    $tempFilePath = [System.IO.Path]::GetTempFileName()
                    Get-AzStorageBlobContent -Container $bucket -Blob $blobKey -Destination $tempFilePath -Context $storageContext -Force

                   
                    $bytes = [System.IO.File]::ReadAllBytes($tempFilePath)

                    
                    $isBinary = $true 

                    if ($isBinary) {
                        $restoredContent = $bytes
                    }
                    else {
                        $restoredContent = [System.Text.Encoding]::UTF8.GetString($bytes)
                    }
                    Remove-Item $tempFilePath -Force
                }
                else {
                    if ($isMinio -eq 1) {
                        $endpoint = "http://localhost:9000"
                    } else {
                        $endpoint = $null
                    }
                    $tempFilePath = [System.IO.Path]::GetTempFileName()
                    if ($endpoint) {
                        Read-S3Object -BucketName $bucket -Key $blobKey -EndpointUrl $endpoint -File $tempFilePath -Force
                    }
                    else {
                        Read-S3Object -BucketName $bucket -Key $blobKey -File $tempFilePath -Force
                    }
                    $bytes = [System.IO.File]::ReadAllBytes($tempFilePath)
                    $isBinary = $true

                    if ($isBinary) {
                        $restoredContent = $bytes
                    } else {
                        $restoredContent = [System.Text.Encoding]::UTF8.GetString($bytes)
                    }

                    Remove-Item $tempFilePath -Force
                }

                if ($null -eq $restoredContent) {
                    # If for some reason we have no content, skip
                    continue
                }

                # We must handle binary vs. string differently in T-SQL:
                if ($restoredContent -is [System.Byte[]]) {
                    # Convert bytes to hex for direct insertion
                    $hexString = ($restoredContent | ForEach-Object ToString "x2") -join ""
                    if ($isIdColumnInt) {
                        $sql = "UPDATE $tableName
                                SET $contentColumnName = 0x$hexString,
                                    externalStorageProvider = NULL
                                WHERE $primaryKeyColumnName = $id"
                    } else {
                        $sql = "UPDATE $tableName
                                SET $contentColumnName = 0x$hexString,
                                    externalStorageProvider = NULL
                                WHERE $primaryKeyColumnName = '$id'"
                    }
                    $msmDatabase.ExecuteNonQuery($sql)
                }
                else {
                    $escapedValue = $restoredContent.Replace("'", "''")
                    if ($isIdColumnInt) {
                        $sql = "UPDATE $tableName
                                SET $contentColumnName = '$escapedValue',
                                    externalStorageProvider = NULL
                                WHERE $primaryKeyColumnName = $id"
                    } else {
                        $sql = "UPDATE $tableName
                                SET $contentColumnName = '$escapedValue',
                                    externalStorageProvider = NULL
                                WHERE $primaryKeyColumnName = '$id'"
                    }
                    $msmDatabase.ExecuteNonQuery($sql)
                }

                $successCount++

            }
            catch {
                $failedCount++
                Write-Host "An exception occurred while processing $primaryKeyColumnName = $id: $_"
            }
            finally {
                $progress++
                $percentageComplete = ($progress / $rows.Count) * 100
                Write-Progress -Activity "Restore in Progress" -Status "$percentageComplete% Complete:" -PercentComplete $percentageComplete
            }
        }

        Write-Host "Total items to restore was $totalItemsToRestore"
        Write-Host "Restore completed. Successfully restored $successCount/$($rows.Count) items in this batch..."

        $remaining = $msmDatabase.ExecuteWithResults(
            "SELECT COUNT(*) as totalItems 
             FROM $tableName
             WHERE externalStorageProvider IS NOT NULL
               AND $contentColumnName IS NULL;"
        ).Tables[0].totalItems
        if ($remaining -eq 0) {
            $hasResults = 0
        }
    }
}

$isAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole] "Administrator")
if (!$isAdmin) {
    try {
        Write-Host "Starting elevated PowerShell instance."
        $newProcess = New-Object System.Diagnostics.ProcessStartInfo "PowerShell"
        $newProcess.Arguments = @(
            "-NoProfile",
            "-NoLogo", 
            $myInvocation.MyCommand.Definition,
            "-storageAccountKey `"$storageAccountKey`" ",
            "-storageAccountName `"$storageAccountName`" ",
            "-isAzure `"$isAzure`" ",
            "-dbServer `"$dbServer`" ",
            "-dbName `"$dbName`" ",
            "-dbUser `"$dbUser`" ",
            "-dbPassword `"$dbPassword`" ",
            "-awsProfile `"$awsProfile`" ",
            "-createBucket `"$createBucket`" ",
            "-bucketName `"$bucketName`" ",
            "-tables `"$tables`" "
        )
        $newProcess.Verb = "runas"
        [System.Diagnostics.Process]::Start($newProcess)
    }
    catch {
        Write-Host "Unable to start elevated PowerShell instance."
    }
    finally {
        exit
    }
}

[System.Net.ServicePointManager]::SecurityProtocol = [System.Net.SecurityProtocolType]"Ssl3,Tls,Tls11,Tls12"


$sqlServerModules = Get-Module -ListAvailable -Name SqlServer
if ($sqlServerModules) {
    Import-Module SqlServer
}

[Reflection.Assembly]::LoadWithPartialName("Microsoft.SqlServer.Smo") | Out-Null

if (!$sqlServerModules) {
    try {
        New-Object Microsoft.SqlServer.Management.SMO.Database | Out-Null
    } catch {
        Write-Host "Installing SqlServer module..."
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
        Write-Host "Installing AZ Powershell module..."
        Install-Module -Name Az -Repository PSGallery -Force
        Import-Module Az
    }
}


if ($isAzure -ne 1) {
    $cmdOutput = aws --version
    if (!$cmdOutput.StartsWith("aws-cli")) {
        Write-Host "AWS CLI not found! Please install AWS CLI and retry. https://awscli.amazonaws.com/AWSCLIV2.msi"
        exit
    }

    $awsPowerShellModules = Get-Module -ListAvailable -Name AWSPowerShell
    if ($awsPowerShellModules) {
        Import-Module AWSPowerShell
    }
    if (!$awsPowerShellModules) {
        try {
            Set-AWSCredential
        } catch {
            Write-Host "Installing AWSPowerShell module..."
            Install-Module AWSPowerShell -Scope CurrentUser
            Import-Module AWSPowerShell
        }
    }

    $awsProfile = if (!$awsProfile) {"default"} else {$awsProfile}
    if ((Get-AWSCredential -ListProfileDetail | Where-Object {$_.ProfileName -eq $awsProfile}).count -eq 1) {
        Set-AWSCredential -ProfileName $awsProfile
        Set-DefaultAWSRegion (aws configure get region --profile $awsProfile)
        $region = (Get-DefaultAWSRegion)
        if ((Get-DefaultAWSRegion).count -eq 0) {
            Write-Host "Please set a default region on the AWS profile and try again."
            exit
        }
        Write-Host "Using AWS profile: $awsProfile"
        Write-Host "Using AWS region: $($region.Region)"
    }
    else {
        Write-Host "No profile found for $awsProfile. Please create or specify a valid AWS profile."
        exit
    }
}

if ($isMinio -eq 1) {
    if ((Get-S3Bucket -EndpointUrl http://localhost:9000 | Where-Object {$_.BucketName -eq $bucketName}).count -eq 0) {
        Write-Host "Bucket $bucketName not found on MinIO."
        exit
    }
}
elseif ($isAzure -ne 1) {
    if ((Get-S3Bucket | Where-Object {$_.BucketName -eq $bucketName}).count -eq 0) {
        Write-Host "Bucket $bucketName not found on AWS."
        exit
    }
}

if (!$dbServer -or !$dbName -or !$dbUser) {
    Write-Host "-dbServer, -dbName and -dbUser MUST be specified!"
    exit
}

if(!$tables) {
    Write-Host "Please specify tables to restore using -tables table1,table2"
    exit
}

$server = New-Object Microsoft.SqlServer.Management.SMO.Server(
    New-Object Microsoft.SqlServer.Management.Common.ServerConnection($dbServer, $dbUser, $dbPassword)
)
$msmDatabase = New-Object Microsoft.SqlServer.Management.SMO.Database($server, $dbName)

$tableArray = $tables.Split(",")
Foreach ($table in $tableArray) {
    switch($table) {
        "queuedNotification" { 
            Move-DataFromExternalStorage -tableName queuedNotification -primaryKeyColumnName queuedNotificationId -contentColumnName content 
        }
        "attachment" { 
            Move-DataFromExternalStorage -tableName attachment -primaryKeyColumnName attachmentId -contentColumnName content 
        }
        "note" { 
            Move-DataFromExternalStorage -tableName note -primaryKeyColumnName noteIdentifier -contentColumnName content 
        }
        "richTextImage" { 
            Move-DataFromExternalStorage -tableName richTextImage -primaryKeyColumnName fileName -contentColumnName content -isIdColumnInt $false 
        }
        default {
            Write-Host "No restore function defined for table: $table"
        }
    }
}
