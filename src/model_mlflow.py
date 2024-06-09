import mlflow
import mlflow.sklearn
from sklearn import datasets
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error


def main():
    # Load the Iris dataset
    iris = datasets.load_iris()
    X = iris.data
    y = iris.target

    # Split the dataset into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Define the model
    model = LinearRegression()

    # Start an MLflow run
    with mlflow.start_run():
        # Train the model
        model.fit(X_train, y_train)
        
        # Make predictions on the test set
        predictions = model.predict(X_test)
        
        # Calculate the mean squared error
        mse = mean_squared_error(y_test, predictions)
        
        # Log the model
        mlflow.sklearn.log_model(model, "model")
        
        # Log the mean squared error as a metric
        mlflow.log_metric("mse", mse)
        
        # Log parameters (in this case, we don't have any hyperparameters to log)
        mlflow.log_param("model_type", "LinearRegression")
        
        print(f"Mean Squared Error: {mse}")

    # The run is automatically ended when the with block ends


if __name__ == "__main__":
    main()