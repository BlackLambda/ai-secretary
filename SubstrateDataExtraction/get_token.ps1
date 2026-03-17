<#
.SYNOPSIS
Contains several cmdlets that allow for interation with the Topics System
.NOTES
These functions and many of the APIs called are subject to change and aren't supported. Use at your own risk.

Usage:

import-module .\topic-utils.ps1 -Force
#>

# environmental declarations
$SubstrateUri = "https://substrate.office.com"


# Test for ADAL installation
$ScriptDir = if ($PSScriptRoot) { $PSScriptRoot } else { Split-Path -parent $PSCommandPath }
$AdalPath = Join-Path -Path $ScriptDir -ChildPath "Microsoft.IdentityModel.Clients.ActiveDirectory.dll"
Write-Verbose "Looking up Microsoft.IdentityModel.Clients.ActiveDirectory.dll in parent path"
if ((Test-Path $AdalPath) -eq $false)
{
    Write-Warning "Missing Microsoft.IdentityModel.Clients.ActiveDirectory.dll in `"$PSCommandPath`". Exiting.`n "
    exit
}
else {
    Write-Verbose "Loading $AdalPath"
    $bytes = [System.IO.File]::ReadAllBytes($AdalPath)
    [System.Reflection.Assembly]::Load($bytes) | Out-Null
    Add-Type -Path $AdalPath
}

#endregion consts

#region ################################################# Authentication commandlets ##################################################

function Get-UserToken-Text()
{
    <#
    .SYNOPSIS
     Returns SecureString containing Bearer token to be used in REST requests
    .DESCRIPTION
    This function relies on Microsoft.IdentityModel.Clients.ActiveDirectory.dll
    to obtain an authentication token from the local user's cache or by
    requesting a new token from Azure Active Directory.  The process of
    requesting a token from Azure AD may prompt the user for credentials.
    The function will fail if there it is not possible to obtain a time-valid
    token that matches the UPN passed and the service
    .EXAMPLE
     Get-UserToken -upn:nikita@example.com -service:https://microsoft.sharepoint.com

     This command prompts for auth and retrieves a token that can be used
     at https://microsoft.sharepoint.com for nikita@example.com
    .OUTPUTS
    Token formatted as json string. Schema is here schemas.auth.user
    #>

    [CmdletBinding()]
    param(
        # User Principal Name of identity. Defaults to current user's UPN.
        [Parameter(Mandatory=$false)][string]$Upn = "",
        # URI to scope. Default is https://substrate.office.com
        [Parameter(Mandatory=$false)][string]$ServiceUri = $SubstrateUri,
        # Tenant Object ID. Default us "Common"
        [Parameter(Mandatory=$false)][string]$tenant = "common"
    )

    # Fault in current logged in user UPN if needed (i.e. not specified in param or passed token)
    if ("" -eq $Upn) {
        # If UPN isn't passed, resolve cross-platform
        # $IsWindows is only available in PowerShell Core 6.0+, so we check for Windows using other methods
        $isWindowsPlatform = ($PSVersionTable.PSEdition -eq 'Desktop') -or ($IsWindows -eq $true)

        if ($isWindowsPlatform) {
            # Use full path to Windows whoami to avoid conflicts with Unix whoami in PATH
            $Upn = & "$env:SystemRoot\System32\whoami.exe" /upn
        }
        else {
            # Try Azure CLI first (works on Linux/macOS if logged in)
            $azUser = (az account show --query user.name -o tsv 2>$null)
            if (-not [string]::IsNullOrWhiteSpace($azUser)) {
                $Upn = $azUser
            }
            elseif ($env:USER -and $env:DOMAIN) {
                $Upn = "$($env:USER)@$($env:DOMAIN)"
            }
            elseif ($env:USER -and $env:USERDOMAIN) {
                $Upn = "$($env:USER)@$($env:USERDOMAIN)"
            }
        }
        if ($null -eq $Upn -or [string]::IsNullOrWhiteSpace($Upn)){
            Write-Error "No UPN passed as parameter or available from current user." -ErrorAction:Stop
        }
        Write-Verbose "No user specified, using UPN for current user $Upn"
    }

    # get constants outta the way
    $authority = "https://login.microsoftonline.com/common"
    $clientId  = "d3590ed6-52b3-4102-aeff-aad2292ab01c"  # ID for Microsoft Office
    $redirectUri = "urn:ietf:wg:oauth:2.0:oob" # No redirect needed; result returned Out Of Band

    $tokenJson = $null # This is what the function ends up returning

    $authContext = [Microsoft.IdentityModel.Clients.ActiveDirectory.AuthenticationContext]::new($authority, $false)

    # If a token that meets the requirements is already cached then the user will not be prompted.
    $promptBehaviour = [Microsoft.IdentityModel.Clients.ActiveDirectory.PlatformParameters]::new(0)  # Auto = 0 - Acquire token will prompt the user for credentials when necessary

    # RequiredDisplayableId = 2 - When a UserIdentifier of this type is passed in a token acquisition operation, the operation
    #                             is guaranteed to return a token issued for the user with corresponding UserIdentifier.DisplayableId (UPN)
    $username = [Microsoft.IdentityModel.Clients.ActiveDirectory.UserIdentifier]::new($Upn, 2)
    Write-Verbose "Acquiring token for $Upn, URI $ServiceUri"

    [Microsoft.IdentityModel.Clients.ActiveDirectory.AuthenticationResult]$authResult = $authContext.AcquireTokenAsync($ServiceUri, $clientId, $redirectUri, $promptBehaviour, $username).Result

    # Due to magic of reflection and async behaviour, $authResult is always null, so need to check cache for token match
    Write-Verbose "Checking token cache for match"
    $tokens=$authContext.TokenCache.ReadItems()

    foreach ($t in $tokens){
        $currentTime = [DateTime]::UtcNow

        # Is there a valid token that matches input?
        if (($t.Resource -eq $ServiceUri) -and ($t.DisplayableId -eq $Upn) -and ($t.ExpiresOn.UtcDateTime -gt $currentTime)){
            $newTokenObj = @{
                aud = $t.Resource
                appid = $t.ClientId
                oid = $t.UniqueId
                tid = $t.TenantId
                upn = $t.DisplayableId
                token = $t.AccessToken
                expires_on = $t.ExpiresOn.UtcDateTime.ToString("yyyy-MM-ddTHH:mm:ssZ")
            }
            $tokenJson = ConvertTo-Json $newTokenObj
            Write-Verbose "Got token for $Upn, URI $ServiceUri, expiring at $($t.ExpiresOn.UtcDateTime) UTC. (It's $currentTime UTC.)"
        }
        else {
            Write-Verbose "Skipping token $($t.DisplayableId), URI $($t.Resource), expiring at $($t.ExpiresOn.UtcDateTime) UTC. (It's $currentTime UTC.)"
        }
    }
    if ($null -eq $tokenJson){
        Write-Error "No token present for user value $Upn" -ErrorAction:Stop
    }

    # Make sure to encode as SecureString on way out
    return $tokenJson
}
