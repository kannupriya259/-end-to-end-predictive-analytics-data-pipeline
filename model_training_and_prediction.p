import psycopg2
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score

# Redshift Configuration
host = "your-redshift-cluster-endpoint"
port = 5439
dbname = "your_database"
user = "your_user"
password = "your_password"

# Connect to Redshift
conn = psycopg2.connect(
    host=host, port=port, dbname=dbname, user=user, password=password
)
query = "SELECT normalized_feature1, normalized_feature2, is_high_label FROM your_table_name"
df = pd.read_sql(query, conn)
conn.close()

# Prepare Data
X = df[["normalized_feature1", "normalized_feature2"]]
y = df["is_high_label"]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train Model
model = LogisticRegression()
model.fit(X_train, y_train)

# Evaluate Model
y_pred = model.predict(X_test)
accuracy = accuracy_score(y_test, y_pred)
print(f"Model Accuracy: {accuracy}")

# Save Predictions
df_test = X_test.copy()
df_test["prediction"] = y_pred
df_test.to_csv("predictions.csv", index=False)
