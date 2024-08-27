#include <gtest/gtest.h>
#include "runtime/local/kernels/map.h" // Include your implementation
#include "DenseMatrix.h" // Include the matrix implementation

// Define a sample user-defined function (UDF) for testing
double udf(double x) {
    return x * 2.0; // Example: doubling each element
}

// Test applying the UDF to a specific row of a matrix
TEST(MapOpTest, ApplyUDFToSpecificRow) {
    // Create a sample 3x3 matrix
    DenseMatrix<double>* input = DataObjectFactory::create<DenseMatrix<double>>(3, 3, false);
    input->set(0, 0, 1.0);
    input->set(0, 1, 2.0);
    input->set(0, 2, 3.0);
    input->set(1, 0, 4.0);
    input->set(1, 1, 5.0);
    input->set(1, 2, 6.0);
    input->set(2, 0, 7.0);
    input->set(2, 1, 8.0);
    input->set(2, 2, 9.0);

    DenseMatrix<double>* result = nullptr;
    bool isMatrix = true;
    bool isRow = true;
    int64_t index = 1; // Apply to the second row (index 1)

    // Apply the map operation
    Map<DenseMatrix<double>, DenseMatrix<double>>::apply(result, input, reinterpret_cast<void*>(&udf), isMatrix, isRow, index, nullptr);

    // Verify the output
    EXPECT_NE(result, nullptr);
    EXPECT_EQ(result->get(1, 0), 8.0);
    EXPECT_EQ(result->get(1, 1), 10.0);
    EXPECT_EQ(result->get(1, 2), 12.0);
}

// Test applying the UDF to a specific column of a matrix
TEST(MapOpTest, ApplyUDFToSpecificColumn) {
    // Create a sample 3x3 matrix (similar to above)
    DenseMatrix<double>* input = DataObjectFactory::create<DenseMatrix<double>>(3, 3, false);
    // ... (set matrix values as above)

    DenseMatrix<double>* result = nullptr;
    bool isMatrix = true;
    bool isRow = false; // Indicate column operation
    int64_t index = 2; // Apply to the third column (index 2)

    // Apply the map operation
    Map<DenseMatrix<double>, DenseMatrix<double>>::apply(result, input, reinterpret_cast<void*>(&udf), isMatrix, isRow, index, nullptr);

    // Verify the output
    EXPECT_NE(result, nullptr);
    EXPECT_EQ(result->get(0, 2), 6.0);
    EXPECT_EQ(result->get(1, 2), 12.0);
    EXPECT_EQ(result->get(2, 2), 18.0);
}

// Test applying the UDF to an invalid row index
TEST(MapOpTest, ApplyUDFToInvalidRowIndex) {
    DenseMatrix<double>* input = DataObjectFactory::create<DenseMatrix<double>>(3, 3, false);
    DenseMatrix<double>* result = nullptr;
    bool isMatrix = true;
    bool isRow = true; // Indicate row operation
    int64_t index = 5; // Invalid row index

    // Apply the map operation with invalid index
    Map<DenseMatrix<double>, DenseMatrix<double>>::apply(result, input, reinterpret_cast<void*>(&udf), isMatrix, isRow, index, nullptr);

    // Verify the output
    EXPECT_EQ(result, nullptr); // Expect result to be null due to invalid index
}

// Test applying the UDF with scalar output
TEST(MapOpTest, ApplyUDFScalarOutput) {
    // Create a sample 3x3 matrix (similar to above)
    DenseMatrix<double>* input = DataObjectFactory::create<DenseMatrix<double>>(3, 3, false);
    // ... (set matrix values as above)

    DenseMatrix<double>* result = nullptr;
    bool isMatrix = false; // Scalar output
    bool isRow = true; // Could be row or column in this case, testing scalar
    int64_t index = 0; // Apply to the first row (index 0)

    // Apply the map operation
    Map<DenseMatrix<double>, DenseMatrix<double>>::apply(result, input, reinterpret_cast<void*>(&udf), isMatrix, isRow, index, nullptr);

    // Verify the output
    EXPECT_NE(result, nullptr);
    EXPECT_EQ(result->get(0, 0), 12.0); // (2*1.0 + 2*2.0 + 2*3.0) assuming UDF sums the row
}
