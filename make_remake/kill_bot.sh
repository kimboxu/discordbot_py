#!/bin/bash
pid=$(pgrep -f "python3 discordbot.py")
if [ -n "$pid" ]; then
    kill -9 $pid
    echo "Discord bot killed (PID: $pid)"
else
    echo "Discord bot not running"
fi