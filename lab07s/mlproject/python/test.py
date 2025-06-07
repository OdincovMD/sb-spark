#!/opt/anaconda/envs/bd9/bin/python3

import sys
import pickle
import base64

# Загрузка модели
with open("lab07.model", "r") as f:
    model_string = f.read()

model_data = pickle.loads(base64.b64decode(model_string.encode("utf-8")))
model = model_data["model"]
vectorizer = model_data["vectorizer"]

features = []

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    x_part = line.replace("features=", "")
    features.append(x_part)

X = vectorizer.transform(features)
preds = model.predict(X)

# Вывод предсказаний
for pred in preds:
    print(pred)
