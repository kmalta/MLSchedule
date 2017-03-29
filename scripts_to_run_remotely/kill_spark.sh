#!/bin/bash
kill $(ps -ef | grep "spark" | awk '{print $2}')