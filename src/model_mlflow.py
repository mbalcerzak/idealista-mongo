import mlflow
import mlflow.sklearn
from sklearn import datasets
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression, Ridge
from sklearn.metrics import mean_squared_error
from sklearn.feature_extraction.text import TfidfVectorizer
import pandas as pd
import numpy as np

from src.preprocessing import clean_description

def main():
    df = pd.read_csv("output/processed_data1.csv")
    print(list(df))

    # df = df.head(10)
    df = df[["price", "description_clean"]]

    # Display the data
    print(df.head())

    # Initialize the TF-IDF Vectorizer
    vectorizer = TfidfVectorizer()

    # Fit and transform the descriptions
    X = vectorizer.fit_transform(df['description_clean'])

    # Convert the TF-IDF matrix to a DataFrame
    tfidf_df = pd.DataFrame(X.toarray(), columns=vectorizer.get_feature_names_out())

    # Display the TF-IDF features
    print(tfidf_df.head())

    # Target variable
    y = df['price']

    # Split the data into training and test sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Initialize the Ridge regression model
    model = Ridge(alpha=1.0)

    with mlflow.start_run():
    # Train the model
        model.fit(X_train, y_train)

        # Evaluate the model
        score = model.score(X_test, y_test)
        print(f"Model R^2 Score: {score:.2f}")

        # Log parameters
        mlflow.log_param("alpha", model.alpha)

        # Log the R^2 score
        mlflow.log_metric("r2_score", score)

        # Log the model
        mlflow.sklearn.log_model(model, "ridge_model")

        # Get the feature names (words)
        feature_names = vectorizer.get_feature_names_out()

        # Get the coefficients from the trained model
        coefficients = model.coef_

        # Create a DataFrame for the feature importance
        importance_df = pd.DataFrame({'feature': feature_names, 'importance': coefficients})

        # Sort the DataFrame by importance in descending order
        importance_df = importance_df.sort_values(by='importance', ascending=False)

        # Log the top 10 words with the most influence on the price
        for i, row in importance_df.head(20).iterrows():
            mlflow.log_metric(f"importance_{row['feature']}", row['importance'])
            print(f"importance_{row['feature']}", row['importance'])


if __name__ == "__main__":
    main()