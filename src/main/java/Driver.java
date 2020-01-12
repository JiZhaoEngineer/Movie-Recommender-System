public class Driver {

    public static void main (String[] args) throws Exception {

        DataDividedByUser f1 = new DataDividedByUser();
        co_occurrenceMatrix f2 = new co_occurrenceMatrix();
        Normalize f3 = new Normalize();
        Multiplication f4 = new Multiplication();
        Sum f5 = new Sum();

        String raw_data = args[0];
        String data_divide_by_user = args[1];
        String co_ocMatrix = args[2];
        String normalize_matrix = args[3];
        String subMultiplication = args[4];
        String final_result = args[5];

        String[] path1 = {raw_data, data_divide_by_user};
        String[] path2 = {data_divide_by_user, co_ocMatrix};
        String[] path3 = {co_ocMatrix, normalize_matrix};
        String[] path4 = {normalize_matrix, raw_data, subMultiplication};
        String[] path5 = {subMultiplication, final_result};

        f1.main(path1);
        f2.main(path2);
        f3.main(path3);
        f4.main(path4);
        f5.main(path5);
    }
}
