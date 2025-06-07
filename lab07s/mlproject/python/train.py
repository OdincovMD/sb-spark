import sys
import traceback
import pickle
import base64
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.pipeline import Pipeline
from sklearn.model_selection import GridSearchCV

def main():
    try:
        lines = [line.strip() for line in sys.stdin if line.strip()]
        if not lines:
            raise ValueError("Зашёл пустой батч")

        records = []
        for line in lines:
            parts = line.split("\t")
            if len(parts) != 2:
                print(f"[WARN] Невалидная строка {line}", file=sys.stderr)
                continue
            feature = parts[0].replace("features=", "")
            label = parts[1].replace("label=", "")
            records.append({"features": feature, "label": label})

        if not records:
            raise ValueError("Зашёл батч из невалдиных строк")

        df = pd.DataFrame(records)

        pipeline = Pipeline([
            ('vectorizer', TfidfVectorizer()),
            ('classifier', RandomForestClassifier(random_state=42))
        ])

        param_grid = {
            'vectorizer__max_df': [0.75, 1.0],
            'vectorizer__ngram_range': [(1,1), (1,2)],
            'classifier__n_estimators': [50, 100],
            'classifier__max_depth': [None, 10, 20]
        }

        grid_search = GridSearchCV(pipeline, param_grid, cv=3, n_jobs=-1, verbose=1)

        grid_search.fit(df['features'], df['label'])

        best_model = grid_search.best_estimator_

        model_bytes = pickle.dumps(best_model)
        model_base64 = base64.b64encode(model_bytes).decode('utf-8')

        print(model_base64)

    except Exception:
        traceback.print_exc(file=sys.stderr)
        sys.exit(2)

if __name__ == "__main__":
    main()
