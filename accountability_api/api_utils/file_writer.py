import csv


def to_csv(arr, filename="result.csv"):
    with open(filename, "w") as file:
        writer = csv.writer(file)
        for row in arr:
            writer.writerow(row)


def to_html(arr, filename="result.html"):
    fp = open(filename, "w")
    contents = [
        "<!DOCTYPE html>"
        "<html>"
        "<body style=\"font-size:12px\">"
        "<table>"
        "<style>"
        "td{padding-left: 5px;padding-right: 5px;}"
        "</style>"
    ]
    for row in arr:
        contents.append("<tr>")
        for elem in row:
            contents.append(f"<td>{elem}</td>")
        contents.append("</tr>")
    contents.append("</table>")
    contents.append("</body>")
    contents.append("</html>")
    contents = "".join(contents)
    fp.write(contents)
    fp.close()
