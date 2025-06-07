import sys
import pickle
import base64
import traceback

def main():
    try:
        model_path = sys.argv[1]

        # Читаем модель из файла (base64)
        with open(model_path, "r") as f:
            model_b64 = f.read()

        # Декодируем и загружаем сериализованную модель (Pipeline)
        model = pickle.loads(base64.b64decode(model_b64.encode("utf-8")))

        features = []
        for line in sys.stdin:
            line = line.strip()
            features.append(line)

        if not features:
            sys.exit(0)

        # model — это sklearn Pipeline, он умеет принимать raw текст и делать трансформацию + predict
        preds = model.predict(features)

        for pred in preds:
            print(pred)

    except Exception:
        traceback.print_exc(file=sys.stderr)
        sys.exit(2)

if __name__ == "__main__":
    main()
