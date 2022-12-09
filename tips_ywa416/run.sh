spark-submit general.py test_data your_output/etl_figures
spark-submit overview.py test_data your_output
spark-submit location.py test_data your_output
python3 location_viz.py your_output your_output/location_result
spark-submit model_regressor.py test_data your_output/model_regressor
spark-submit model_classifier.py test_data your_output/model_classifier
