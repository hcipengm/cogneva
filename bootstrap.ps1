# Cogneva 元启动入口（Windows）。
# 用法（管理员 PowerShell）:
#   iwr -useb https://raw.githubusercontent.com/hcipengm/cogneva/main/bootstrap.ps1 | iex
# K3s 不能原生运行于 Windows，本脚本自动准备 WSL2 + Ubuntu 作为 Linux 运行层，
# 然后在 WSL 内执行与 Linux/macOS 完全相同的一键命令。所有依赖装在 WSL 内，宿主零侵入。
#Requires -RunAsAdministrator
[CmdletBinding()]
param(
    # 强制走/不走国内镜像；缺省自动探测（探 rustup 分发域，不通即国内）
    [ValidateSet('auto', '1', '0')]
    [string]$CnMirror = 'auto'
)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Write-Step([string]$Msg) { Write-Host "[bootstrap] $Msg" }

# 与 README 完全同一条入口命令；CN 模式 Gitee 优先
$EntryCmdIntl = '(curl -fsSL -m 15 https://raw.githubusercontent.com/hcipengm/cogneva/main/bootstrap.sh || curl -fsSL -m 15 https://gitee.com/hcipengm/cogneva/raw/main/bootstrap.sh) | sh'
$EntryCmdCn   = '(curl -fsSL -m 15 https://gitee.com/hcipengm/cogneva/raw/main/bootstrap.sh || curl -fsSL -m 15 https://raw.githubusercontent.com/hcipengm/cogneva/main/bootstrap.sh) | sh'

function Resolve-CnMirror {
    if ($script:CnMirror -eq '1') { return '1' }
    if ($script:CnMirror -eq '0') { return '0' }
    try {
        Invoke-WebRequest -Uri 'https://static.rust-lang.org/rustup/release-stable.toml' `
            -Method Head -TimeoutSec 5 -UseBasicParsing | Out-Null
        return '0'
    }
    catch {
        Write-Step '检测到受限网络（rustup 分发域不可达），启用国内镜像...'
        return '1'
    }
}

function Test-WslInstalled {
    # wsl --status 在未安装/未启用时会抛错或非零退出
    $prev = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    wsl.exe --status 2>$null | Out-Null
    $ok = ($LASTEXITCODE -eq 0)
    $ErrorActionPreference = $prev
    return $ok
}

function Ensure-Wsl {
    if (Test-WslInstalled) {
        Write-Step 'WSL 已启用'
        return
    }
    Write-Step '安装 WSL2 + Ubuntu（wsl --install -d Ubuntu）...'
    wsl.exe --install -d Ubuntu
    # wsl --install 在需要重启时返回 3010 或打印重启提示
    if ($LASTEXITCODE -eq 3010) {
        Write-Host ''
        Write-Host '[bootstrap] WSL 组件已安装，需要重启 Windows 后生效。' -ForegroundColor Yellow
        Write-Host '[bootstrap] 重启后请重新运行本脚本（幂等，会自动继续）。' -ForegroundColor Yellow
        exit 0
    }
    if ($LASTEXITCODE -ne 0) {
        # 部分版本已成功但返回非零；复查一次
        if (-not (Test-WslInstalled)) {
            throw "wsl --install 失败（exit $LASTEXITCODE）。请手动运行: wsl --install -d Ubuntu"
        }
    }
    Write-Host ''
    Write-Host '[bootstrap] WSL 已安装。若系统提示需要重启，请重启后重新运行本脚本。' -ForegroundColor Yellow
}

function Test-UbuntuReady {
    $prev = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    wsl.exe -d Ubuntu -u root -- sh -c 'true' 2>$null | Out-Null
    $ok = ($LASTEXITCODE -eq 0)
    $ErrorActionPreference = $prev
    return $ok
}

function Ensure-Ubuntu {
    if (Test-UbuntuReady) {
        Write-Step 'Ubuntu 发行版可用'
        return
    }
    Write-Step '等待 Ubuntu 初始化（首次启动可能触发交互式建用户，本脚本以 root 运行可跳过）...'
    wsl.exe --install -d Ubuntu --no-launch 2>$null | Out-Null
    $deadline = (Get-Date).AddMinutes(3)
    while ((Get-Date) -lt $deadline) {
        if (Test-UbuntuReady) { return }
        Start-Sleep -Seconds 5
    }
    throw 'Ubuntu 初始化超时。请手动运行一次 "wsl -d Ubuntu" 完成初始化后重跑本脚本。'
}

function Set-UbuntuCnApt {
    Write-Step '国内模式：Ubuntu 内换国内 apt 源（多候选探活）...'
    # apt 候选：TUNA → USTC → 阿里云。探测在 Windows 侧做（WSL 裸机无 curl，
    # 而 CN 下默认 archive.ubuntu.com 不可达装不了 curl——先有鸡先有蛋问题）
    $aptHost = 'mirrors.tuna.tsinghua.edu.cn'
    foreach ($h in @('mirrors.tuna.tsinghua.edu.cn', 'mirrors.ustc.edu.cn', 'mirrors.aliyun.com')) {
        try {
            Invoke-WebRequest -Uri "https://$h/ubuntu/dists/noble/Release" -TimeoutSec 5 -UseBasicParsing -OutFile $null
            $aptHost = $h; break
        } catch { Write-Host "[bootstrap] apt 镜像不可达，换下一个: $h" }
    }
    Write-Host "[bootstrap] apt 镜像: $aptHost"
    $sed = "sed -i 's|http://archive.ubuntu.com/ubuntu|https://$aptHost/ubuntu|g; s|http://security.ubuntu.com/ubuntu|https://$aptHost/ubuntu|g' /etc/apt/sources.list; if [ -f /etc/apt/sources.list.d/ubuntu.sources ]; then sed -i 's|http://archive.ubuntu.com/ubuntu|https://$aptHost/ubuntu|g; s|http://security.ubuntu.com/ubuntu|https://$aptHost/ubuntu|g' /etc/apt/sources.list.d/ubuntu.sources; fi"
    wsl.exe -d Ubuntu -u root -- sh -c "$sed; apt-get update -qq; apt-get install -y curl ca-certificates"
    if ($LASTEXITCODE -ne 0) { throw 'Ubuntu 内 apt 换源/装 curl 失败' }
}

$cn = Resolve-CnMirror
Ensure-Wsl
Ensure-Ubuntu
if ($cn -eq '1') { Set-UbuntuCnApt }

$entry = if ($cn -eq '1') { $EntryCmdCn } else { $EntryCmdIntl }
Write-Step "在 WSL Ubuntu 内执行与 Linux 完全相同的一键命令，COGNEVA_CN_MIRROR=$cn 已透传..."
wsl.exe -d Ubuntu -u root -- sh -c "COGNEVA_CN_MIRROR=$cn $entry"
if ($LASTEXITCODE -ne 0) { throw "WSL 内引导失败（exit $LASTEXITCODE）" }

Write-Host ''
Write-Host '[bootstrap] 完成！Cogneva 已在 WSL Ubuntu 内运行。' -ForegroundColor Green
Write-Host 'WSL2 默认开启 localhostForwarding，浏览器直接访问: http://localhost:8080'
Write-Host '常用命令: wsl -d Ubuntu（进 Linux 层）| wsl --shutdown（停止）'
Start-Process 'http://localhost:8080'
