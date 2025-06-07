#!/opt/anaconda/envs/bd9/bin/python3
import sys
import pickle
import base64

from sklearn.linear_model import LogisticRegression
from sklearn.feature_extraction.text import CountVectorizer

features = []
labels = []

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split("\t")
    x_part = parts[0].replace("features=", "")
    y_part = parts[1].replace("label=", "")

    features.append(x_part)
    labels.append(y_part)

vectorizer = CountVectorizer()
X = vectorizer.fit_transform(features)

model = LogisticRegression(max_iter=200)
model.fit(X, labels)

# Сохраняем и модель, и vectorizer
full_model = {
    "vectorizer": vectorizer,
    "model": model
}

model_string = base64.b64encode(pickle.dumps(full_model)).decode('utf-8')
print(model_string)
