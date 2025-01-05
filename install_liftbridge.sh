#!/usr/bin/env bash

# Liftbridge Installer

header() {
    cat 1>&2 <<EOF
Liftbridge Installer

Website: https://github.com/liftbridge-io/liftbridge
Releases: https://github.com/liftbridge-io/liftbridge/tree/master/releases/file

EOF
}

check_cmd() {
    command -v "$1" > /dev/null 2>&1
}

check_tools() {
    Tools=("curl" "grep" "cut" "tar" "uname" "chmod" "mv" "rm")

    for tool in "${Tools[@]}"; do
        if ! check_cmd "$tool"; then
            echo "Aborted, missing $tool, sorry!"
            exit 6
        fi
    done
}

install_liftbridge() {
    trap 'echo "Aborted, error $? in command: $BASH_COMMAND"; trap ERR; exit 1' ERR
    install_path="/usr/local/bin"
    liftbridge_os="unsupported"
    liftbridge_arch="unknown"

    header
    check_tools

    if [[ -n "$PREFIX" ]]; then
        install_path="$PREFIX/bin"
    fi

    if [[ ! -d $install_path ]]; then
        install_path="/usr/bin"
    fi

    ((EUID)) && sudo_cmd="sudo"

    liftbridge_bin="liftbridge"
    liftbridge_dl_ext=".tar.gz"
    version="1.9.0"

    # Determine architecture
    unamem="$(uname -m)"
    if [[ $unamem == *aarch64* ]]; then
        liftbridge_arch="arm64"
    elif [[ $unamem == *64* ]]; then
        liftbridge_arch="amd64"
    elif [[ $unamem == *armv6l* ]]; then
        liftbridge_arch="armv6"
    elif [[ $unamem == *armv7l* ]]; then
        liftbridge_arch="armv7"
    else
        echo "Aborted, unsupported or unknown architecture: $unamem"
        return 2
    fi

    # Determine OS
    unameu="$(uname | tr '[:lower:]' '[:upper:]')"
    if [[ $unameu == *DARWIN* ]]; then
        liftbridge_os="darwin"
    elif [[ $unameu == *LINUX* ]]; then
        liftbridge_os="linux"
    else
        echo "Aborted, unsupported or unknown OS: $unameu"
        return 6
    fi

    echo "Downloading Liftbridge for ${liftbridge_os}/${liftbridge_arch}..."
    liftbridge_file="${liftbridge_bin}_${version}_termux_${liftbridge_os}_${liftbridge_arch}${liftbridge_dl_ext}"
    liftbridge_url="https://github.com/ashit1303/liftbridge/releases/${liftbridge_file}"

    dl="$PREFIX/tmp/${liftbridge_file}"
    rm -rf -- "$dl"

    echo "Downloading $liftbridge_url..."
    curl -fsSL "$liftbridge_url" -o "$dl"

    echo "Extracting..."
    case "$liftbridge_file" in
        *.tar.gz) tar -xzf "$dl" -C "$PREFIX/tmp" "$liftbridge_bin" ;;
    esac
    chmod +x "$PREFIX/tmp/$liftbridge_bin"

    echo "Installing Liftbridge to $install_path (may require password)..."
    mv "$PREFIX/tmp/$liftbridge_bin" "$install_path/$liftbridge_bin"
    rm -- "$dl"

    echo "Successfully installed Liftbridge"
    trap ERR
    return 0
}

install_liftbridge $@