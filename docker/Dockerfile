ARG WINDOWS_VERSION=20H2
FROM mcr.microsoft.com/windows/servercore:$WINDOWS_VERSION

ARG PYTHON_VERSION=3.8.9
ARG PYTHON_RELEASE=3.8.9
# if this is called "PIP_VERSION", pip explodes with "ValueError: invalid truth value '<VERSION>'"
ARG PYTHON_PIP_VERSION=22.1.2
# https://github.com/pypa/get-pip
ARG PYTHON_GET_PIP_URL=https://github.com/pypa/get-pip/raw/d59197a3c169cef378a22428a3fa99d33e080a5d/get-pip.py

USER ContainerAdministrator

WORKDIR C:\\Temp
SHELL ["powershell", "-Command", "$ErrorActionPreference = 'Stop'; $ProgressPreference = 'Continue'; $verbosePreference='Continue';"]

# Python setup
RUN [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.SecurityProtocolType]::Tls12; \
    Invoke-WebRequest -UseBasicParsing -Uri "https://www.python.org/ftp/python/$env:PYTHON_RELEASE/python-$env:PYTHON_VERSION-embed-amd64.zip" -Out 'Python.zip'; \
    Expand-Archive -Path "Python.zip"; \
    Invoke-WebRequest -UseBasicParsing -Uri "$env:PYTHON_GET_PIP_URL" -OutFile 'Python\get-pip.py'; \
    [String]::Format('@set PYTHON_PIP_VERSION={0}', $env:PYTHON_PIP_VERSION) | Out-File -FilePath 'Python\pipver.cmd' -Encoding ASCII;

RUN $FileVer = [System.Version]::Parse([System.Diagnostics.FileVersionInfo]::GetVersionInfo('Python\python.exe').ProductVersion); \
    $Postfix = $FileVer.Major.ToString() + $FileVer.Minor.ToString(); \
    Remove-Item -Path "Python\python$Postfix._pth"; \
    Expand-Archive -Path "Python\python$Postfix.zip" -Destination "Python\Lib"; \
    Remove-Item -Path "Python\python$Postfix.zip"; \
    New-Item -Type Directory -Path "Python\DLLs";

FROM mcr.microsoft.com/powershell:nanoserver-20h2

# Continue Python setup
COPY --from=0 C:\\Temp\\Python C:\\Python

RUN mkdir C:\\Temp

USER ContainerAdministrator

RUN setx.exe /m TEMP C:\Temp

SHELL ["pwsh", "-Command", "$ErrorActionPreference = 'Stop'; $ProgressPreference = 'Continue'; $verbosePreference='Continue';"]

ARG GITBASH_URL=https://github.com/git-for-windows/git/releases/download/v2.36.1.windows.1
ARG GITBASH_FILE=MinGit-2.36.1-64-bit.zip
ARG GITBASH_DEST='C:\Git'

RUN [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.SecurityProtocolType]::Tls12; \
    Write-Host "Downloading Git..."; \
    Invoke-WebRequest -UseBasicParsing -Uri "$env:GITBASH_URL/$env:GITBASH_FILE" -OutFile "C:\\Temp\\$env:GITBASH_FILE" -verbose; \
    Get-ChildItem -Path "C:\\Temp\\" -Recurse | Unblock-File; \
    Expand-Archive -Path "C:\\Temp\\$env:GITBASH_FILE" -Destination $env:GITBASH_DEST;

ADD .\\docker\\pwiz-3.0.21209.zip C:\\Temp
RUN Expand-Archive -Path "C:\\Temp\\pwiz-3.0.21209.zip" -Destination "C:\\"; \
    Test-Path "C:\\pwiz-3.0.21209\\msconvert.exe";

SHELL ["cmd", "/S", "/C"]

ENV PYTHONPATH C:\\Python;C:\\Python\\Scripts;C:\\Python\\DLLs;C:\\Python\\Lib;C:\\Python\\Lib\\plat-win;C:\\Python\\Lib\\site-packages
RUN setx.exe /m PATH "%PATH%;%PYTHONPATH%;%GITBASH_DEST%\cmd" &&  \
    setx.exe /m PYTHONPATH %PYTHONPATH% &&  \
    setx.exe /m PIP_CACHE_DIR C:\Users\ContainerUser\AppData\Local\pip\Cache && \
    reg.exe ADD HKLM\SYSTEM\CurrentControlSet\Control\FileSystem /v LongPathsEnabled /t REG_DWORD /d 1 /f

RUN echo Verifying git install ... && \
    echo   git --version && \
    git --version && \
    echo Complete.

# https://soooprmx.com/archives/6471
RUN assoc .py=Python.File && \
    assoc .pyc=Python.CompiledFile && \
    assoc .pyd=Python.Extension && \
    assoc .pyo=Python.CompiledFile && \
    assoc .pyw=Python.NoConFile && \
    assoc .pyz=Python.ArchiveFile && \
    assoc .pyzw=Python.NoConArchiveFile && \
    ftype Python.ArchiveFile="C:\Python\python.exe" "%1" %* && \
    ftype Python.CompiledFile="C:\Python\python.exe" "%1" %* && \
    ftype Python.File="C:\Python\python.exe" "%1" %* && \
    ftype Python.NoConArchiveFile="C:\Python\pythonw.exe" "%1" %* && \
    ftype Python.NoConFile="C:\Python\pythonw.exe" "%1" %*

RUN call C:\Python\pipver.cmd && \
    %COMSPEC% /s /c "echo Installing pip==%PYTHON_PIP_VERSION% ..." && \
    %COMSPEC% /s /c "C:\Python\python.exe C:\Python\get-pip.py --disable-pip-version-check --no-cache-dir pip==%PYTHON_PIP_VERSION%" && \
    type c:\python\pipver.cmd && \
    echo Removing ... && \
    del /f /q C:\Python\get-pip.py C:\Python\pipver.cmd && \
    echo Verifying install ... && \
    echo   python --version && \
    python --version && \
    echo Verifying pip install ... && \
    echo   pip --version && \
    pip --version && \
    echo Complete.

RUN pip install virtualenv

USER ContainerUser

RUN git clone https://github.com/metabolomics-us/monitor.git

WORKDIR C:\\monitor\\

RUN pip install -r requirements.txt

ENTRYPOINT ["cmd"]

#CMD ["python", "launch", "-t", "-s", "--debug"]