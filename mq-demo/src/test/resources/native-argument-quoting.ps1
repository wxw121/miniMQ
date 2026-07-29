param(
    [Parameter(Mandatory = $true)]
    [string]$RepositoryRoot
)

$ErrorActionPreference = "Stop"

Add-Type -TypeDefinition @'
using System;
using System.ComponentModel;
using System.Runtime.InteropServices;

public static class NativeCommandLine
{
    [DllImport("shell32.dll", SetLastError = true)]
    private static extern IntPtr CommandLineToArgvW(
        [MarshalAs(UnmanagedType.LPWStr)] string commandLine,
        out int argumentCount);

    [DllImport("kernel32.dll")]
    private static extern IntPtr LocalFree(IntPtr memory);

    public static string[] Parse(string commandLine)
    {
        int argumentCount;
        IntPtr arguments = CommandLineToArgvW(commandLine, out argumentCount);
        if (arguments == IntPtr.Zero)
        {
            throw new Win32Exception(Marshal.GetLastWin32Error());
        }

        try
        {
            string[] result = new string[argumentCount];
            for (int index = 0; index < argumentCount; index++)
            {
                IntPtr argument = Marshal.ReadIntPtr(arguments, index * IntPtr.Size);
                result[index] = Marshal.PtrToStringUni(argument);
            }
            return result;
        }
        finally
        {
            LocalFree(arguments);
        }
    }
}
'@

$cases = @(
    [pscustomobject]@{ Name = "empty value"; Value = "" },
    [pscustomobject]@{ Name = "embedded quotes"; Value = 'say "hello"' },
    [pscustomobject]@{ Name = "spaces"; Value = "two words" },
    [pscustomobject]@{ Name = "backslashes"; Value = 'C:\MiniMQ\data' },
    [pscustomobject]@{ Name = "backslash before quote"; Value = 'say \"hello"' },
    [pscustomobject]@{ Name = "trailing backslash"; Value = 'C:\Program Files\MiniMQ\' }
)

foreach ($scriptName in @("start-brokers.ps1", "demo-v4-failover.ps1")) {
    $scriptPath = Join-Path (Join-Path $RepositoryRoot "scripts") $scriptName
    $tokens = $null
    $parseErrors = $null
    $ast = [System.Management.Automation.Language.Parser]::ParseFile(
        $scriptPath,
        [ref]$tokens,
        [ref]$parseErrors)
    if ($parseErrors.Count -gt 0) {
        throw "$scriptName has PowerShell parse errors: $($parseErrors -join '; ')"
    }

    $functions = @($ast.FindAll({
                param($node)
                $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
                    $node.Name -eq "ConvertTo-NativeArgument"
            }, $true))
    if ($functions.Count -ne 1) {
        throw "$scriptName must define ConvertTo-NativeArgument exactly once."
    }
    Invoke-Expression $functions[0].Extent.Text

    foreach ($case in $cases) {
        $encoded = ConvertTo-NativeArgument -Value $case.Value
        $parsed = [NativeCommandLine]::Parse("minimq.exe $encoded")
        if ($parsed.Length -ne 2 -or $parsed[1] -cne $case.Value) {
            $expectedJson = ConvertTo-Json $case.Value -Compress
            $actualJson = ConvertTo-Json @($parsed) -Compress
            throw "$scriptName failed '$($case.Name)': expected $expectedJson; argv=$actualJson; encoded=$encoded"
        }
    }
}

Write-Output "Native argument quoting tests passed for start-brokers.ps1 and demo-v4-failover.ps1."
