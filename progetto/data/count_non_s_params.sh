#!/bin/bash
DIR_STORICO="storico_Anno_Stazione_Parametro"
DIR_CUR="Stazione_Parametro_AnnoMese"
cd "$DIR_CUR"
for filename in * ; do
    
    RES=$( cat $filename | awk -F ',' '{print $5}' | sort | uniq -c | grep -v "VALIDAZIONE" | grep -v "S" )
    if [[ -n $RES ]]; then
        echo "$filename : $RES"
        #echo $RES
        
    fi
done
cd ..
