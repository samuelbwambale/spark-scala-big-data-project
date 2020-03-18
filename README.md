# spark-scala-big-data-project

STEPS

- Download a dataset (.csv file) from the site https://vincentarelbundock.github.io/Rdatasets/datasets.html
- Select a categorical variable and a numeric variable and form the key-value pair and create a pairRDD called “population”.
- Compute the mean mpg and variance for each category
- Create the sample for bootstrapping taking 25% of the population without replacement
- Create a “resampledData” taking 100% of the sample with replacement.
- Compute the mean and variance for each category
