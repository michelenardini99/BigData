#!/bin/bash

# --- CONFIGURAZIONE ---
BUCKET="unibo-bd2526-mnardini"
KEY="datasets/flights_final.csv"
ORIGINAL_CSV="datasets/flights_final.csv"
FOLDER="./temp_parts"

echo "Preparazione del set di dati..."

FILES=($(ls $FOLDER/part_* | tail -n 30))
FIRST_FILE=${FILES[0]}
OTHER_FILES=("${FILES[@]:1}")


head -n 1 "$ORIGINAL_CSV" > header_tmp.csv
cat header_tmp.csv "$FIRST_FILE" > part_1_with_header.tmp
rm header_tmp.csv

echo "Inizializzo il Multipart Upload su S3..."
UPLOAD_ID=$(aws s3api create-multipart-upload --bucket $BUCKET --key $KEY --query 'UploadId' --output text)

if [ "$UPLOAD_ID" == "None" ] || [ -z "$UPLOAD_ID" ]; then
    echo "Errore: Verifica credenziali!"
    exit 1
fi

echo '{"Parts": []}' > parts.json

echo "Carico la Parte 1 (Header + $FIRST_FILE)..."
ETAG=$(aws s3api upload-part --bucket $BUCKET --key $KEY --part-number 1 --body part_1_with_header.tmp --upload-id $UPLOAD_ID --query 'ETag' --output text)

jq -n --arg et "$ETAG" '{"PartNumber": 1, "ETag": $et}' > tmp.json
jq '.Parts += [input]' parts.json tmp.json > parts_new.json && mv parts_new.json parts.json
rm part_1_with_header.tmp

PART_NUMBER=2
for FILE in "${OTHER_FILES[@]}"
do
    echo "Carico $FILE (Parte $PART_NUMBER di 20)..."
    ETAG=$(aws s3api upload-part --bucket $BUCKET --key $KEY --part-number $PART_NUMBER --body "$FILE" --upload-id $UPLOAD_ID --query 'ETag' --output text)

    jq -n --arg pn "$PART_NUMBER" --arg et "$ETAG" '{"PartNumber": ($pn | tonumber), "ETag": $et}' > tmp.json
    jq '.Parts += [input]' parts.json tmp.json > parts_new.json && mv parts_new.json parts.json

    PART_NUMBER=$((PART_NUMBER + 1))
done

echo "Finalizzazione in corso..."
aws s3api complete-multipart-upload --multipart-upload file://parts.json --bucket $BUCKET --key $KEY --upload-id $UPLOAD_ID

echo "--------------------------------------------------"
echo "SUCCESSO! File pronto: s3://$BUCKET/$KEY"
echo "--------------------------------------------------"